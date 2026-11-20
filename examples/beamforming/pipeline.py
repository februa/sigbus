"""Finite beamforming prototype with periodic state display. No DSP correctness is claimed.

Run from the repository root (see README).
Nodes reserve endpoint groups; successful AP passes commit before deferred send.
"""
from dataclasses import dataclass
import argparse
import json
from operator import index
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event as Flag, Timer

import numpy as np

from sigbus import (Bus, StateSpec, StreamSpec, FIFOPolicy,
                    InProcessTransport,
                    FileTransport, SharedMemoryTransport)
from sigbus.worker import Worker, Stage, ModuleSpec, Periodic, RunResult
from sigbus.module import Module as BaseModule


@dataclass(frozen=True)
class Config:
    blocks: int = 24
    samples: int = 8
    channels: int = 4
    directions: int = 6
    frequencies: int = 8
    frequencies_per_tick: int = 2
    average_samples: int = 20
    signal_hz: float = 200
    sound_hz: float = 20
    weights_hz: float = 100
    poll_interval: float = .001
    display_interval: float = .005
    display: str = 'average'
    timeout: float = 10
    quiet: bool = False

    def __post_init__(self):
        for name in ('blocks', 'samples', 'channels', 'directions', 'frequencies', 'frequencies_per_tick', 'average_samples'):
            if index(getattr(self, name)) < 1:
                raise ValueError(f'{name} must be positive')
        for name in ('signal_hz', 'sound_hz', 'weights_hz', 'poll_interval', 'display_interval', 'timeout'):
            value = getattr(self, name)
            if not np.isfinite(value) or value <= 0:
                raise ValueError(f'{name} must be finite and positive')
        if self.display not in ('average', 'latest'):
            raise ValueError('display must be average or latest')


class ProcessingError(Exception):
    """Expected calculation failure: publish a fallback, without reprocessing."""


class Module(BaseModule):
    """Demo-specific counters and fallback handling; lifecycle is library-owned."""
    def __init__(self, bus, specs, config):
        super().__init__(bus, specs, interval=config.poll_interval)
        self.config = config
        self.done = Flag()
        self.published = 0
        self.processing_errors = 0

    def calculate(self, out, function, *args, fallback=0):
        try:
            value = function(*args)
        except ProcessingError:
            self.processing_errors += 1
            value = fallback
        out.payload[...] = value  # Define the entire output, including on failure.
        return value

    def run_once(self):
        return self.step()

    def finish(self):
        if not self.drain():
            raise InterruptedError('pipeline stopped before publication completed')
        self.done.set()
        return RunResult.FINISHED


def position_stub(channels):
    return np.arange(channels)[:, None] * [1., 0., 0.]


def sound_stub(sequence):
    return 1500. + sequence


def steering_stub(position, sound):
    # Shape/dependencies only; not a physical steering vector.
    return 1 + position[:, 0] / sound


def signal_stub(sequence, position, sound):
    return sequence + 1 + position[:, 0] + (sound - 1500.) / 100


def weights_stub(covariance, steering):
    # No inversion or MVDR normalization.
    return steering / (1 + abs(covariance[0, 0]))


def beam_stub(signal, weights):
    return signal[:, :1] * weights[:, :, 0].real.mean(axis=0)


class Position(Module):
    def initialize(self):
        self.out = self.publisher('position')
        self.node = self.make_node(publishers=(self.out,))

    def step(self):
        with self.node.acquire(timestamp=0) as frames:
            self.calculate(frames[self.out], position_stub, self.config.channels)
        self.published += 1
        return self.finish()


class Sound(Module):
    def __init__(self, bus, specs, config, *, signal_done):
        super().__init__(bus, specs, config)
        self.signal_done = signal_done

    def initialize(self):
        self.out = self.publisher('sound')
        self.node = self.make_node(publishers=(self.out,))

    def step(self):
        if self.signal_done.is_set():
            return self.finish()
        with self.node.acquire(timestamp=self.published) as frames:
            self.calculate(frames[self.out], sound_stub, self.published)
        self.published += 1
        return RunResult.PROCESSED


class Steering(Module):
    def __init__(self, bus, specs, config, *, sound_done):
        super().__init__(bus, specs, config)
        self.sound_done = sound_done

    def initialize(self):
        self.sound = self.subscriber('sound')
        self.position = self.subscriber('position')
        self.out = self.publisher('steering')
        self.node = self.make_node(subscribers=(self.sound, self.position), publishers=(self.out,))
        self.sound_generations = []

    def step(self):
        # Snapshot completion before acquisition: a final publication cannot be missed.
        finished = self.sound_done.is_set()
        with self.node.acquire() as frames:
            sound, position = frames[self.sound], frames[self.position]
            self.calculate(frames[self.out], steering_stub, position.payload, sound.payload)
            self.sound_generations.append(sound.generation)
        self.published += 1
        return self.finish() if finished else RunResult.PROCESSED


class Signal(Module):
    def initialize(self):
        self.position = self.subscriber('position')
        self.sound = self.subscriber('sound')
        self.out = self.publisher('signal', policy=FIFOPolicy(overflow='block', no_readers='proceed'))
        self.node = self.make_node(subscribers=(self.position, self.sound), publishers=(self.out,))

    def step(self):
        with self.node.acquire(timestamp=self.published) as frames:
            self.calculate(frames[self.out], signal_stub, self.published,
                           frames[self.position].payload, frames[self.sound].payload)
        self.published += 1
        return self.finish() if self.published == self.config.blocks else RunResult.PROCESSED


def covariance_stub(signal, previous):
    """Exercise persistent EMA state, without computing an actual covariance."""
    sample = float(np.mean(signal.real))
    return .9 * previous + .1 * np.eye(previous.shape[0]) * sample


class Covariance(Module):
    def initialize(self):
        self.signal = self.subscriber('signal')
        self.out = self.publisher('covariance')
        self.node = self.make_node(subscribers=(self.signal,), publishers=(self.out,))
        self.received_sequences = []
        self.integrated = np.zeros((self.config.channels, self.config.channels), dtype='c16')

    def step(self):
        with self.node.acquire() as frames:
            signal = frames[self.signal]
            # Previous value is an AP-owned array, never a returned payload view.
            updated = self.calculate(frames[self.out], covariance_stub,
                                     signal.payload, self.integrated, fallback=self.integrated)
            sequence = signal.timestamp
        self.integrated = updated
        self.received_sequences.append(sequence)
        self.published += 1
        return self.finish() if self.published == self.config.blocks else RunResult.PROCESSED


class Weights(Module):
    def __init__(self, bus, specs, config, *, covariance_done, steering_done):
        super().__init__(bus, specs, config)
        self.covariance_done = covariance_done
        self.steering_done = steering_done

    def initialize(self):
        self.covariance = self.subscriber('covariance')
        self.steering = self.subscriber('steering')
        self.out = self.publisher('weights')
        self.node = self.make_node(subscribers=(self.covariance, self.steering), publishers=(self.out,))
        self.last_input_generations = None
        self.inputs = self.outputs = None
        self.next_frequency = 0
        self.calculation_passes = []

    def step(self):
        if self.inputs is None:
            # Finite-demo termination: start one last full calculation after both
            # producers finish. No value-change detection is involved.
            self.final_pass = (self.covariance_done.is_set()
                               and self.steering_done.is_set())
            self.node.reserve("subscribe")
            self.inputs = self.node.checkout()
            covariance = self.inputs[self.covariance]
            self.node.reserve("publish", timestamp=covariance.timestamp)
            self.outputs = self.node.checkout()
            self.next_frequency = 0
            self.pass_ticks = 0
            self.pass_failed = False

        covariance = self.inputs[self.covariance]
        steering = self.inputs[self.steering]
        out = self.outputs[self.out]
        end = min(self.next_frequency + self.config.frequencies_per_tick,
                  self.config.frequencies)
        try:
            for frequency in range(self.next_frequency, end):
                # Stub: reuse the same covariance/steering at each frequency.
                # The frequency factor makes unfinished slices observable in tests.
                out.payload[frequency] = weights_stub(covariance.payload, steering.payload) * (1 + frequency / 100)
        except ProcessingError:
            self.processing_errors += 1
            out.payload[...] = 0  # Never expose a mixture of completed and stale slices.
            self.pass_failed = True
            end = self.config.frequencies
        self.pass_ticks += 1
        self.next_frequency = end
        if end < self.config.frequencies:
            return RunResult.PROCESSED  # Hold both input snapshots and output slot.

        generations = (covariance.generation, steering.generation)
        self.node.commit()   # Only a complete filter (or full fallback) is published.
        self.node.complete()
        self.inputs = self.outputs = None
        self.last_input_generations = generations
        self.calculation_passes.append({'ticks': self.pass_ticks,
                                        'input_generations': generations,
                                        'fallback': self.pass_failed})
        self.published += 1
        return self.finish() if self.final_pass else RunResult.PROCESSED

    def finalize(self):
        try:
            super().finalize()  # Aborts unfinished checkout on cancellation.
        finally:
            self.inputs = self.outputs = None


class Beam(Module):
    def initialize(self):
        self.signal = self.subscriber('signal')  # independent FIFO reader
        self.weights = self.subscriber('weights')
        self.out = self.publisher('beam', policy=FIFOPolicy(overflow='block', no_readers='proceed'))
        self.node = self.make_node(subscribers=(self.signal, self.weights), publishers=(self.out,))
        self.weight_generations = []
        self.received_sequences = []

    def step(self):
        with self.node.acquire() as frames:
            signal, weights = frames[self.signal], frames[self.weights]
            self.calculate(frames[self.out], beam_stub, signal.payload, weights.payload)
            sequence, generation = signal.timestamp, weights.generation
        self.received_sequences.append(sequence)
        self.weight_generations.append(generation)
        self.published += 1
        return self.finish() if self.published == self.config.blocks else RunResult.PROCESSED


class PowerAverager:
    """Accumulate sample rows across frames; retain sums, never input views."""
    def __init__(self, window_samples, directions):
        self.window_samples = window_samples
        self.power_sum = np.zeros(directions)
        self.sample_count = 0

    def add(self, power):
        windows = []
        offset = 0
        while offset < len(power):
            take = min(self.window_samples - self.sample_count, len(power) - offset)
            self.power_sum += power[offset:offset + take].sum(axis=0)
            self.sample_count += take
            offset += take
            if self.sample_count == self.window_samples:
                windows.append(self.flush())
        return windows

    def flush(self):
        """Return the remaining window, or None when empty; no zero padding."""
        if not self.sample_count:
            return None
        window = {'samples': self.sample_count,
                  'power': (self.power_sum / self.sample_count).tolist()}
        self.power_sum.fill(0)
        self.sample_count = 0
        return window


class Spectrum(Module):
    """Consume every beam block; publish the current display value as state."""
    def initialize(self):
        self.beam = self.subscriber('beam')
        self.out = self.publisher('spectrum')
        self.node = self.make_node(subscribers=(self.beam,), publishers=(self.out,))
        self.averager = PowerAverager(self.config.average_samples, self.config.directions)
        self.windows = []
        self.received_sequences = []

    def publish_window(self, window, timestamp):
        if window is None:
            return
        self.node.reserve("publish", timestamp=timestamp)
        frames = self.node.checkout()
        frames[self.out].payload['samples'] = window['samples']
        frames[self.out].payload['power'] = window['power']
        self.node.commit()
        self.windows.append(window)
        self.published += 1

    def step(self):
        self.node.reserve("subscribe")
        frames = self.node.checkout()
        beam = frames[self.beam]
        timestamp = beam.timestamp
        power = abs(beam.payload) ** 2  # Independent array, not a borrowed view.
        self.node.complete()
        self.received_sequences.append(timestamp)
        if self.config.display == 'latest':
            self.publish_window({'samples': 1, 'power': power[-1].tolist()}, timestamp)
        else:
            for window in self.averager.add(power):
                self.publish_window(window, timestamp)
        if len(self.received_sequences) == self.config.blocks:
            if self.config.display == 'average':
                self.publish_window(self.averager.flush(), timestamp)
            return self.finish()
        return RunResult.PROCESSED


class Display(Module):
    def __init__(self, bus, specs, config, *, spectrum_done):
        super().__init__(bus, specs, config)
        # Finite-demo termination only; this flag never triggers a redraw.
        self.spectrum_done = spectrum_done

    def initialize(self):
        self.spectrum = self.subscriber('spectrum')
        self.node = self.make_node(subscribers=(self.spectrum,))
        self.windows = []
        self.received_sequences = []

    def step(self):
        # Read the current state every display tick, even if unchanged. The first
        # acquisition waits for initial publication; no update detection is used.
        finished = self.spectrum_done.is_set()
        with self.node.acquire() as frames:
            spectrum = frames[self.spectrum]
            timestamp = spectrum.timestamp
            window = {'samples': int(spectrum.payload['samples']),
                      'power': spectrum.payload['power'].tolist()}
        self.received_sequences.append(timestamp)
        self.windows.append(window)
        if not self.config.quiet:
            print(f"spectrum ({window['samples']} samples): {np.round(window['power'], 3)}")
        return self.finish() if finished else RunResult.PROCESSED



class Pipeline:
    def __init__(self, transport, config):
        self.config = config
        self.bus = Bus(transport)
        self.specs = {
            'position': StateSpec('position', (config.channels, 3), 'f8', slots=1),
            'sound': StateSpec('sound', (), 'f8'),
            'steering': StateSpec('steering', (config.directions, config.channels), 'c16'),
            'signal': StreamSpec('signal', (config.samples, config.channels), 'c16', slots=5, rate_hz=config.signal_hz),
            'covariance': StateSpec('covariance', (config.channels, config.channels), 'c16'),
            'weights': StateSpec('weights', (config.frequencies, config.directions, config.channels), 'c16', slots=3),
            'beam': StreamSpec('beam', (config.samples, config.directions), 'c16', slots=5, rate_hz=config.signal_hz),
            'spectrum': StateSpec('spectrum', (), np.dtype([
                ('samples', 'i8'), ('power', 'f8', (config.directions,))])),
        }
        # name: (Module class, rate_hz, completion-argument -> producer name).
        # Producers precede their completion dependents for construction only;
        # every Stage still runs concurrently and connects data in initialize().
        definitions = {
            'signal': (Signal, config.signal_hz, {}),
            'position': (Position, 1 / config.poll_interval, {}),
            'sound': (Sound, config.sound_hz, {'signal_done': 'signal'}),
            'steering': (Steering, config.sound_hz, {'sound_done': 'sound'}),
            'covariance': (Covariance, 1 / config.poll_interval, {}),
            'weights': (Weights, config.weights_hz,
                        {'covariance_done': 'covariance', 'steering_done': 'steering'}),
            'beam': (Beam, 1 / config.poll_interval, {}),
            'spectrum': (Spectrum, 1 / config.poll_interval, {}),
            'display': (Display, 1 / config.display_interval, {'spectrum_done': 'spectrum'}),
        }
        self.modules = {}
        for name, (module_type, _, dependencies) in definitions.items():
            completion = {argument: self.modules[producer].done
                          for argument, producer in dependencies.items()}
            self.modules[name] = module_type(self.bus, self.specs, config, **completion)

        self.worker = Worker([
            Stage(name, [ModuleSpec(name, self.modules[name])], Periodic(rate_hz))
            for name, (_, rate_hz, _) in definitions.items()
        ])

    def run(self):
        expired = Flag()
        def timeout():
            expired.set()
            self.worker.stop()
        timer = Timer(self.config.timeout, timeout)
        timer.start()
        try:
            reports = self.worker.run()
        finally:
            timer.cancel()
            timer.join()
        if expired.is_set():
            raise TimeoutError('prototype did not finish before the deadline')
        if not all(module.done.is_set() for module in self.modules.values()):
            raise RuntimeError('prototype stopped before draining all stages')
        return {
            'published': {name: m.published for name, m in self.modules.items() if name != 'display'},
            'received': {name: self.modules[name].received_sequences for name in ('covariance', 'beam', 'spectrum', 'display')},
            'steering_sound_generations': self.modules['steering'].sound_generations,
            'beam_weight_generations': self.modules['beam'].weight_generations,
            'weights_calculation_passes': self.modules['weights'].calculation_passes,
            'final_weights_inputs': self.modules['weights'].last_input_generations,
            'windows': self.modules['spectrum'].windows,
            'displayed_windows': self.modules['display'].windows,
            'idle_calls': {name: r.stats.idle for name, r in reports.items()},
            'processing_errors': {name: m.processing_errors for name, m in self.modules.items()},
        }


def run_demo(config=Config(), transport_kind='inprocess'):
    # Demo resources are private and disposable; never remove a user's data directory.
    with TemporaryDirectory(prefix='sigbus-prototype-') as directory:
        if transport_kind == 'inprocess':
            transport = InProcessTransport()
        elif transport_kind == 'file':
            transport = FileTransport(Path(directory))
        elif transport_kind == 'shared_memory':
            transport = SharedMemoryTransport('prototype', directory=directory)
        else:
            raise ValueError('unknown transport')
        pipeline = Pipeline(transport, config)
        try:
            return pipeline.run()
        finally:
            for spec in pipeline.specs.values():
                transport.remove(spec)  # no force: also catches leaked endpoints
            transport.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--transport', choices=['inprocess', 'file', 'shared_memory'], default='inprocess')
    parser.add_argument('--display', choices=['average', 'latest'], default='average')
    parser.add_argument('--blocks', type=int, default=24)
    parser.add_argument('--frequencies', type=int, default=8)
    parser.add_argument('--frequencies-per-tick', type=int, default=2)
    parser.add_argument('--average-samples', type=int, default=20)
    parser.add_argument('--display-interval', type=float, default=.005)
    parser.add_argument('--quiet', action='store_true')
    args = parser.parse_args()
    config = Config(blocks=args.blocks, average_samples=args.average_samples,
                    frequencies=args.frequencies, frequencies_per_tick=args.frequencies_per_tick,
                    display=args.display, display_interval=args.display_interval, quiet=args.quiet)
    print(json.dumps(run_demo(config, args.transport), indent=2))


if __name__ == '__main__':
    main()
