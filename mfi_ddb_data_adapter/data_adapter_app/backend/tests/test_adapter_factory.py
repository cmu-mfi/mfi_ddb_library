"""Polling lifecycle tests using controlled adapters without network connections."""

import importlib.util
import queue
import sys
import threading
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest
from pydantic import BaseModel


@pytest.fixture
def make_factory(monkeypatch):
    class Config(BaseModel):
        pass

    class Adapter:
        NAME = "Test"
        SELF_UPDATE = True
        SCHEMA = Config

        def __init__(self, config):
            self.config = config

    streamers = []

    class Streamer:
        SCHEMA = Config

        def __init__(self, config, adapter, stream_on_update):
            self.polls = queue.Queue()
            self.finish_poll = threading.Semaphore(0)
            self.workers = set()
            self.disconnect_count = 0
            streamers.append(self)

        def poll_and_stream_data(self, polling_rate_hz):
            worker = threading.current_thread()
            self.workers.add(worker)
            self.polls.put((polling_rate_hz, worker))
            if not self.finish_poll.acquire(timeout=2):
                raise TimeoutError("Test did not release the polling operation")

        def disconnect(self):
            self.disconnect_count += 1

    # Load the real factory with only the network-facing library replaced.
    library = ModuleType("mfi_ddb")
    library.data_adapters = SimpleNamespace(BaseDataAdapter=Adapter)
    library.Streamer = Streamer
    monkeypatch.setitem(sys.modules, "mfi_ddb", library)
    source = Path(__file__).parents[1] / "app/services/adapter_factory.py"
    spec = importlib.util.spec_from_file_location("adapter_factory_under_test", source)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    factories = []

    def make(**kwargs):
        factory = module.AdapterFactory(adp_name="Test", **kwargs)
        factories.append(factory)
        return factory

    yield make

    for factory in factories:
        factory.disconnect()
    for streamer in streamers:
        for _ in range(4):
            streamer.finish_poll.release()
    for streamer in streamers:
        for worker in tuple(streamer.workers):
            worker.join(timeout=1)
            assert not worker.is_alive(), "Polling worker did not shut down"


def assert_no_poll(streamer):
    with pytest.raises(queue.Empty):
        streamer.polls.get(timeout=0.05)


def test_polling_resumes_after_repeated_pauses(make_factory):
    factory = make_factory(polling_rate_hz=5)
    factory.connect_and_stream()
    streamer = factory.streamer
    rate, worker = streamer.polls.get(timeout=1)
    assert rate == 5

    for _ in range(3):
        factory.pause_streaming()
        assert factory.is_connected
        assert not factory.is_streaming
        streamer.finish_poll.release()
        assert_no_poll(streamer)

        factory.resume_streaming()
        rate, resumed_worker = streamer.polls.get(timeout=1)
        assert rate == 5
        assert resumed_worker is worker
        assert factory.is_streaming
        assert factory.streamer is streamer
        assert streamer.disconnect_count == 0


def test_resume_during_inflight_poll_does_not_duplicate_worker(make_factory):
    factory = make_factory()
    factory.connect_and_stream()
    streamer = factory.streamer
    _, worker = streamer.polls.get(timeout=1)

    for _ in range(3):
        factory.pause_streaming()
        factory.resume_streaming()
        factory.resume_streaming()
        assert_no_poll(streamer)
        streamer.finish_poll.release()
        _, resumed_worker = streamer.polls.get(timeout=1)
        assert resumed_worker is worker


def test_disconnect_wakes_paused_worker(make_factory):
    factory = make_factory()
    factory.connect_and_stream()
    streamer = factory.streamer
    _, worker = streamer.polls.get(timeout=1)
    factory.pause_streaming()
    streamer.finish_poll.release()
    assert_no_poll(streamer)
    assert worker.is_alive()

    factory.disconnect()
    worker.join(timeout=1)

    assert not worker.is_alive()
    assert not factory.is_connected
    assert not factory.is_streaming
    assert not factory.poll_streaming.is_set()
    assert streamer.disconnect_count == 1
    assert_no_poll(streamer)


def test_reconnect_does_not_reactivate_previous_worker(make_factory):
    factory = make_factory()
    factory.connect_and_stream()
    old_streamer = factory.streamer
    _, old_worker = old_streamer.polls.get(timeout=1)

    # Leave the old poll in flight until after the new connection starts.
    factory.disconnect()
    factory.connect_and_stream()
    new_streamer = factory.streamer
    _, new_worker = new_streamer.polls.get(timeout=1)
    old_streamer.finish_poll.release()
    old_worker.join(timeout=1)

    assert not old_worker.is_alive()
    assert new_worker is not old_worker
    assert new_streamer is not old_streamer
    assert_no_poll(old_streamer)
    assert_no_poll(new_streamer)
    new_streamer.finish_poll.release()
    _, next_worker = new_streamer.polls.get(timeout=1)
    assert next_worker is new_worker
