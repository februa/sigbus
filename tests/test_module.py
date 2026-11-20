from concurrent.futures import ThreadPoolExecutor
from threading import Event

import pytest
from sigbus import Bus, InProcessTransport, StateSpec
from sigbus.worker import ModuleSpec, Periodic, RunResult, Stage, Worker, State
from sigbus.module import Module


def test_manual_work_spans_worker_passes():
    transport = InProcessTransport()
    bus = Bus(transport)
    specs = {'source': StateSpec('source', (2,), 'i8'),
             'result': StateSpec('result', (2,), 'i8')}
    source = bus.publisher(specs['source'])
    result = bus.subscriber(specs['result'])
    with source.acquire(7) as frame:
        frame.payload[...] = 4

    class Calculate(Module):
        def initialize(self):
            self.input = self.subscriber('source')
            self.output = self.publisher('result')
            self.node = self.make_node(subscribers=(self.input,), publishers=(self.output,))
            self.position = 0

        def run_once(self):
            if self.position == 0:
                self.node.reserve()
                self.frames = self.node.checkout()
            self.frames[self.output].payload[self.position] = self.frames[self.input].payload[self.position] * 2
            self.position += 1
            if self.position < 2:
                assert not result.has_frame()
                return RunResult.PROCESSED
            self.node.commit()
            self.node.complete()
            assert self.drain()
            return RunResult.FINISHED

    module = Calculate(bus, specs)
    worker = Worker([Stage('calculate', [ModuleSpec('calculate', module)], Periodic(1 / .001))])
    try:
        worker.run()
        assert module.position == 2
        assert module.input.closed and module.output.closed
        with result.acquire() as frame:
            assert frame.timestamp == 7
            assert (frame.payload == 8).all()
    finally:
        source.close()
        result.close()
        transport.close()


@pytest.mark.parametrize('use_node', [False, True])
def test_worker_stop_interrupts_checkout_without_ap_stop_wrapper(use_node):
    transport = InProcessTransport()
    bus = Bus(transport)
    specs = {'source': StateSpec('source', (2,), 'i8')}
    waiting = Event()

    class Waiting(Module):
        def initialize(self):
            self.input = self.subscriber('source')
            self.target = self.make_node(subscribers=(self.input,)) if use_node else self.input

        def run_once(self):
            self.target.reserve()
            waiting.set()
            self.target.checkout()  # No timeout or custom AP cancellation wiring.
            raise AssertionError('nothing was published')

    module = Waiting(bus, specs)
    worker = Worker([Stage('waiting', [ModuleSpec('waiting', module)], Periodic(1 / .001))])
    with ThreadPoolExecutor() as pool:
        future = pool.submit(worker.run)
        try:
            assert waiting.wait(2)
        finally:
            worker.stop()
        future.result(timeout=3)
    assert worker.state is State.STOPPED
    assert module.input.closed
    transport.close()


def test_module_drain_includes_standalone_partial_batch():
    from sigbus import StreamSpec
    transport = InProcessTransport()
    bus = Bus(transport)
    specs = {'out': StreamSpec('out', (1,), 'i8', batch_size=4, rate_hz=100)}
    module = Module(bus, specs)
    output = module.publisher('out')
    subscriber = bus.subscriber(specs['out'])
    try:
        output.reserve(timestamp=1)
        frame = output.checkout()
        frame.payload[...] = 7
        output.commit()
        assert not subscriber.has_frame()
        assert module.drain()
        with subscriber.acquire() as frame:
            assert frame.payload[0] == 7
    finally:
        module.finalize()
        subscriber.close()
        transport.close()
