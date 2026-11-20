"""Run: .venv/bin/python -m pytest examples/beamforming -q"""
from dataclasses import replace
import numpy as np
import pytest
from sigbus import Bus, InProcessTransport
from sigbus.worker import WorkerError
from .pipeline import Config, Pipeline, run_demo


@pytest.mark.parametrize('transport', ['inprocess', 'file', 'shared_memory'])
@pytest.mark.parametrize('display', ['average', 'latest'])
def test_whole_flow_and_drain(transport, display):
    c = Config(blocks=17, samples=8, average_samples=20, display=display, quiet=True)
    result = run_demo(c, transport)
    expected = list(range(c.blocks))
    assert result['received']['covariance'] == expected
    assert result['received']['beam'] == expected
    assert result['published']['position'] == 1
    assert result['published']['signal'] == result['published']['beam'] == c.blocks
    generations = result['steering_sound_generations']
    assert generations == sorted(generations)
    assert generations[-1] == result['published']['sound']
    assert all(g > 0 for g in result['beam_weight_generations'])
    assert result['final_weights_inputs'] == (result['published']['covariance'], result['published']['steering'])
    expected_ticks = (c.frequencies + c.frequencies_per_tick - 1) // c.frequencies_per_tick
    assert all(p['ticks'] == expected_ticks for p in result['weights_calculation_passes'])
    seen = result['received']['display']
    assert seen == sorted(seen) and seen[-1] == c.blocks - 1
    assert result['received']['spectrum'] == expected
    if display == 'average':
        assert [w['samples'] for w in result['windows']] == [20] * 6 + [16]
    else:
        assert all(w['samples'] == 1 for w in result['windows'])
    assert all(np.isfinite(w['power']).all() for w in result['windows'])


def test_slow_display_does_not_lose_processing_samples():
    c = Config(blocks=32, signal_hz=1000, display_interval=.025, quiet=True)
    result = run_demo(c)
    for reader in ('covariance', 'beam', 'spectrum'):
        assert result['received'][reader] == list(range(32))
    # Backpressure stays inside Node.acquire, without AP-level retry passes.
    assert result['idle_calls']['beam'] == 0
    assert result['idle_calls']['signal'] == 0
    assert sum(w['samples'] for w in result['windows']) == 32 * c.samples


def test_average_crosses_frame_boundary_and_emits_partial_window():
    transport = InProcessTransport()
    app = Pipeline(transport, Config(blocks=2, samples=3, average_samples=4, directions=2, quiet=True))
    spectrum = app.modules['spectrum']
    spectrum.initialize()
    pub = Bus(transport).publisher(app.specs['beam'])
    try:
        for timestamp, values in enumerate(([1, 2, 3], [4, 5, 6])):
            with pub.acquire(timestamp) as frame:
                frame.payload[:] = np.array(values)[:, None]
            spectrum.run_once()
        assert [w['samples'] for w in spectrum.windows] == [4, 2]
        np.testing.assert_allclose(spectrum.windows[0]['power'], [7.5, 7.5])
        np.testing.assert_allclose(spectrum.windows[1]['power'], [30.5, 30.5])
    finally:
        spectrum.finalize()
        pub.close()
        transport.remove(app.specs['spectrum'])
        transport.remove(app.specs['beam'])
        transport.close()


def test_worker_failure_closes_all_endpoints(monkeypatch):
    from . import pipeline
    def fail(*args):
        raise ValueError('stub calculation failed')
    monkeypatch.setattr(pipeline, 'covariance_stub', fail)
    with pytest.raises(WorkerError) as failure:
        run_demo(Config(quiet=True))
    assert isinstance(failure.value.__cause__, ValueError)
    # run_demo's non-force remove would raise if any endpoint was leaked.


def test_watchdog_stops_and_cleans_up():
    with pytest.raises(TimeoutError):
        run_demo(Config(blocks=100, signal_hz=1, timeout=.05, quiet=True))


@pytest.mark.parametrize('kwargs', [{'blocks': 0}, {'samples': 0}, {'display': 'bad'},
                                    {'average_samples': 0}, {'frequencies': 0}, {'frequencies_per_tick': 0}, {'timeout': float('nan')}])
def test_invalid_configuration(kwargs):
    with pytest.raises(ValueError):
        replace(Config(), **kwargs)


def test_latest_display_can_skip_without_blocking_fifo_publisher():
    c = Config(blocks=32, signal_hz=500, display='latest', display_interval=.03, quiet=True)
    result = run_demo(c, 'file')
    assert result['received']['beam'] == list(range(c.blocks))
    assert result['received']['display'][-1] == c.blocks - 1
    assert len(result['received']['display']) < c.blocks


def test_hold_with_only_latest_reader_retains_skipped_frames():
    # Concrete reproduction of the configuration trap found during the prototype.
    from sigbus import StreamSpec, FIFOPolicy, LatestPolicy, NoWritableSlot
    transport = InProcessTransport()
    spec = StreamSpec('hold_trap', (1,), 'f8', slots=3, rate_hz=100)
    pub = Bus(transport).publisher(spec, policy=FIFOPolicy(overflow='block'))
    sub = Bus(transport).subscriber(spec, policy=LatestPolicy())
    try:
        for i in range(3):
            with pub.acquire(i) as frame:
                frame.payload[:] = i
        with sub.acquire() as frame:
            assert frame.timestamp == 2
        with pytest.raises(NoWritableSlot), pub.acquire(3):
            pass
    finally:
        sub.close()
        pub.close()
        transport.remove(spec)
        transport.close()


def test_averager_multiple_windows_and_independent_results():
    from .pipeline import PowerAverager
    averager = PowerAverager(window_samples=3, directions=1)
    assert averager.flush() is None
    power = np.arange(1, 11, dtype=float)[:, None]
    windows = averager.add(power)
    power[:] = 999  # The accumulator must not retain a view of its input.
    assert windows == [
        {'samples': 3, 'power': [2.]},
        {'samples': 3, 'power': [5.]},
        {'samples': 3, 'power': [8.]},
    ]
    assert averager.flush() == {'samples': 1, 'power': [10.]}
    assert averager.flush() is None


def test_processing_error_publishes_previous_covariance(monkeypatch):
    from . import pipeline
    transport = InProcessTransport()
    app = Pipeline(transport, Config(blocks=3, quiet=True))
    covariance = app.modules['covariance']
    covariance.initialize()
    pub = app.bus.publisher(app.specs['signal'])
    sub = app.bus.subscriber(app.specs['covariance'])
    try:
        with pub.acquire(0) as frame:
            frame.payload[...] = 5
        covariance.run_once()
        covariance.node.drain(timeout=1)
        with sub.acquire() as frame:
            previous = frame.payload.copy()

        def fail(*args):
            raise pipeline.ProcessingError('expected processing failure')
        monkeypatch.setattr(pipeline, 'covariance_stub', fail)
        with pub.acquire(1) as frame:
            frame.payload[...] = 999
        covariance.run_once()
        covariance.node.drain(timeout=1)
        with sub.acquire() as frame:
            assert frame.timestamp == 1
            np.testing.assert_array_equal(frame.payload, previous)
        assert covariance.received_sequences == [0, 1]
        assert covariance.processing_errors == 1
        assert not covariance.signal.has_frame()
    finally:
        covariance.finalize()
        pub.close()
        sub.close()
        for spec in app.specs.values():
            transport.remove(spec)
        transport.close()


def test_processing_error_publishes_fixed_value_and_continues(monkeypatch):
    from . import pipeline
    def fail(*args):
        raise pipeline.ProcessingError('beam calculation failed')
    monkeypatch.setattr(pipeline, 'beam_stub', fail)
    result = run_demo(Config(blocks=7, quiet=True))
    assert result['received']['beam'] == list(range(7))
    assert result['received']['spectrum'] == list(range(7))
    assert result['received']['display'][-1] == 6
    assert result['processing_errors']['beam'] == 7
    assert all(np.all(np.array(window['power']) == 0) for window in result['windows'])


def test_publication_retry_does_not_reexecute_processing(monkeypatch):
    from sigbus.transport.storage import InProcessStorage
    from . import pipeline
    original_flush = InProcessStorage.flush_slot
    original_compute = pipeline.covariance_stub
    attempts, computations = [], []

    def flaky(self, slot):
        if self.spec.name == 'covariance' and len(attempts) < 3:
            attempts.append(slot)
            raise OSError('temporary publication failure')
        return original_flush(self, slot)

    def compute(*args):
        computations.append(1)
        return original_compute(*args)

    monkeypatch.setattr(InProcessStorage, 'flush_slot', flaky)
    monkeypatch.setattr(pipeline, 'covariance_stub', compute)
    result = run_demo(Config(blocks=7, quiet=True))
    assert len(attempts) == 3
    assert len(computations) == 7
    assert result['received']['covariance'] == list(range(7))
    assert result['received']['display'][-1] == 6


def test_periodic_display_reads_unchanged_state_again():
    transport = InProcessTransport()
    app = Pipeline(transport, Config(quiet=True))
    display = app.modules['display']
    display.initialize()
    pub = app.bus.publisher(app.specs['spectrum'])
    try:
        with pub.acquire(10) as frame:
            frame.payload['samples'] = 20
            frame.payload['power'] = 3
        display.run_once()
        display.run_once()  # No new publication and no completion/update event.
        assert display.received_sequences == [10, 10]
        assert display.windows[0] == display.windows[1]
        assert not display.done.is_set()
    finally:
        display.finalize()
        pub.close()
        transport.remove(app.specs['spectrum'])
        transport.close()


@pytest.fixture(params=['inprocess', 'file', 'shared_memory'])
def manual_weights(request, tmp_path):
    from sigbus import FileTransport, SharedMemoryTransport
    if request.param == 'inprocess':
        transport = InProcessTransport()
    elif request.param == 'file':
        transport = FileTransport(tmp_path)
    else:
        transport = SharedMemoryTransport('weights-demo', directory=tmp_path)
    app = Pipeline(transport, Config(frequencies=5, frequencies_per_tick=2, quiet=True))
    weights = app.modules['weights']
    weights.initialize()
    covariance = app.bus.publisher(app.specs['covariance'])
    steering = app.bus.publisher(app.specs['steering'])
    result = app.bus.subscriber(app.specs['weights'])
    for endpoint, value in ((covariance, 1), (steering, 2)):
        with endpoint.acquire(1) as frame:
            frame.payload[...] = value
    yield weights, covariance, steering, result
    weights.finalize()
    for endpoint in (covariance, steering, result):
        endpoint.close()
    for spec in app.specs.values():
        transport.remove(spec)
    transport.close()


def test_weights_keep_snapshot_and_publish_only_complete_filter(manual_weights):
    weights, covariance, steering, result = manual_weights
    weights.run_once()
    assert weights.next_frequency == 2
    assert not result.has_frame()
    # Update upstream while computation is in progress: held inputs must not change.
    for endpoint in (covariance, steering):
        with endpoint.acquire(9) as frame:
            frame.payload[...] = 9
    weights.run_once()
    assert not result.has_frame()
    weights.run_once()
    assert weights.drain()
    with result.acquire() as frame:
        assert frame.timestamp == 1
        for frequency in range(5):
            np.testing.assert_allclose(frame.payload[frequency], 1 + frequency / 100)
    assert weights.calculation_passes == [
        {'ticks': 3, 'input_generations': (1, 1), 'fallback': False}]
    weights.run_once()
    weights.run_once()
    with result.acquire() as frame:
        assert frame.generation == 1  # Previous complete value stays readable.
    weights.run_once()
    assert weights.drain()
    with result.acquire() as frame:
        assert frame.generation == 2
        assert frame.timestamp == 9
        np.testing.assert_allclose(frame.payload[-1], .9 * 1.04)


def test_weights_processing_failure_overwrites_entire_partial_output(manual_weights, monkeypatch):
    from . import pipeline
    weights, _, _, result = manual_weights
    weights.run_once()
    def fail(*args):
        raise pipeline.ProcessingError('frequency calculation failed')
    monkeypatch.setattr(pipeline, 'weights_stub', fail)
    weights.run_once()
    assert weights.drain()
    with result.acquire() as frame:
        assert np.all(frame.payload == 0)
    assert weights.processing_errors == 1
    assert weights.calculation_passes[0]['fallback'] is True
    assert weights.inputs is weights.outputs is None


def test_cancel_between_weights_ticks_aborts_without_publication(manual_weights):
    weights, covariance, _, result = manual_weights
    weights.run_once()
    output_view = weights.outputs[weights.out].payload
    weights.stop_event.set()
    weights.finalize()
    assert not result.has_frame()
    assert not output_view.flags.writeable
    assert weights.inputs is weights.outputs is None
    # Still-open upstream endpoint is independent of the cancelled calculation.
    with covariance.acquire(2) as frame:
        frame.payload[...] = 2
