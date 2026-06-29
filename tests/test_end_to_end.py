"""
End-to-end tests for atomic-streams.

Each test uses a real Redis instance via testcontainers. Do not mock Redis,
because the correctness of this library depends on the exact Redis pipeline
and stream behavior that mocks cannot reproduce.
"""

import threading
import time
from typing import Any

import pytest
from redis import Redis
from testcontainers.redis import RedisContainer

from atomic_redis_streams import (
    Persistable,
    PersistableAttribute,
    RedisStreamConsumer,
    TransactionManager,
)


@pytest.fixture(scope="session")
def redis_container():
    """
    Start a Redis container for the test session and stop it when the session ends.
    """
    with RedisContainer() as container:
        yield container


@pytest.fixture(scope="session")
def redis_url(redis_container):
    """
    Return the connection URL for the session-scoped Redis container.
    """
    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(6379)
    return f"redis://{host}:{port}"


@pytest.fixture(autouse=True)
def flush_redis(redis_url):
    """
    Flush all Redis keys after each test to ensure a clean state.
    """
    yield
    Redis.from_url(redis_url).flushall()


# Helpers


def _publish(redis_url: str, stream: str, fields: dict) -> None:
    """
    Append a single entry with the given fields to the named stream.
    """
    client = Redis.from_url(redis_url)
    client.xadd(stream, {k: str(v) for k, v in fields.items()})
    client.close()


def _run_consumer(
    consumer: RedisStreamConsumer, done: threading.Event, timeout: float = 5.0
) -> None:
    """
    Start the consumer, wait until done is set or timeout expires, then stop it.
    """
    consumer.start()
    done.wait(timeout=timeout)
    consumer.stop()


# Tests


def test_messages_delivered_in_order(redis_url):
    """
    Test that messages published to a stream are delivered to the handler in order.
    Publish three messages and run a consumer that collects the received values.
    Assert the handler received them in the exact order they were published.
    """
    done = threading.Event()

    class HandlingClass:
        def __init__(self) -> None:
            self.received: list[str] = []

        def handle(self, message: dict[str, Any], transaction: TransactionManager) -> None:
            self.received.append(message["message"])
            if len(self.received) == 3:
                done.set()

    handling_class = HandlingClass()

    _publish(redis_url, "stream-id", {"message": "1"})
    _publish(redis_url, "stream-id", {"message": "2"})
    _publish(redis_url, "stream-id", {"message": "3"})

    consumer = RedisStreamConsumer("stream-id", "consumer-unique-id", handling_class, redis_url)
    _run_consumer(consumer, done)

    assert handling_class.received == ["1", "2", "3"]


def test_persistable_attributes_restored_on_restart(redis_url):
    """
    Test that a Persistable handler's state is persisted atomically and restored on restart.
    Process three messages, then start a new consumer with a fresh handler and one new message.
    Assert the fresh handler accumulates from the restored state rather than starting from zero.
    """

    done = threading.Event()

    class HandlingClass(Persistable):
        count = PersistableAttribute(default=0)

        @property
        def unique_id(self) -> str:
            return "handler-main"

        def handle(self, message: dict[str, Any], transaction: TransactionManager) -> None:
            self.count += 1
            if self.count >= 3:
                done.set()

    for i in range(3):
        _publish(redis_url, "stream-id", {"id": f"{i}"})

    handler = HandlingClass()
    consumer = RedisStreamConsumer("stream-id", "consumer-id", handler, redis_url)
    _run_consumer(consumer, done)

    assert handler.count == 3

    # Simulate crash: publish one more message and restart with a fresh handler instance
    _publish(redis_url, "stream-id", {"id": "3"})
    done.clear()

    fresh_handler = HandlingClass()
    restarted = RedisStreamConsumer("stream-id", "consumer-id", fresh_handler, redis_url)
    _run_consumer(restarted, done)

    # State is restored (3) + 1 new message = 4. If the state was not restored, the count would be 1
    assert fresh_handler.count == 4


def test_no_duplicate_processing_after_restart(redis_url):
    """
    Test that the consumer does not reprocess messages after a restart.
    Process a first batch, publish a second batch while the consumer is stopped, then restart.
    Assert only the new messages are processed and state accumulates correctly.
    """

    class HandlingClass(Persistable):
        count = PersistableAttribute(default=0)

        @property
        def unique_id(self) -> str:
            return "handler-main"

        def handle(self, message: dict[str, Any], transaction: TransactionManager) -> None:
            self.count += 1

    # Publish a first batch and process it
    for i in range(3):
        _publish(redis_url, "stream-id", {"id": f"{i}"})

    done = threading.Event()
    handler = HandlingClass()

    calls = [0]
    original = handler.handle

    def counting_handle(msg, tx):
        original(msg, tx)
        calls[0] += 1
        if calls[0] == 3:
            done.set()

    handler.handle = counting_handle

    consumer = RedisStreamConsumer("stream-id", "consumer-id", handler, redis_url)
    _run_consumer(consumer, done)
    assert handler.count == 3

    # Publish a second batch while consumer is stopped
    for i in range(3, 6):
        _publish(redis_url, "stream-id", {"id": f"{i}"})

    # Restart with a fresh handler
    done2 = threading.Event()
    fresh_handler = HandlingClass()

    calls2 = [0]
    original2 = fresh_handler.handle

    def counting_handle2(msg, tx):
        original2(msg, tx)
        calls2[0] += 1
        if calls2[0] == 3:
            done2.set()

    fresh_handler.handle = counting_handle2

    consumer2 = RedisStreamConsumer("stream-id", "consumer-id", fresh_handler, redis_url)
    _run_consumer(consumer2, done2)

    # State is restored (3) + 3 new = 6 total; only 3 new messages were processed this run
    assert calls2[0] == 3
    assert fresh_handler.count == 6


def test_downstream_publish_rolled_back_on_failure(redis_url):
    """
    Test that a downstream publish is rolled back if the handler raises an exception.
    Run a consumer whose handler always fails after staging a publish to a second stream.
    Assert the downstream stream remains empty after the failed attempts.
    """

    class HandlingClass:
        def __init__(self, should_fail: bool) -> None:
            self.should_fail = should_fail

        def handle(self, message: dict[str, Any], transaction: TransactionManager) -> None:
            transaction.publish("output-stream", {"id": message["id"]})
            if self.should_fail:
                raise RuntimeError("Transient failure")

    _publish(redis_url, "input-stream", {"id": "1"})

    # Run a consumer whose callback always fails
    failing_handler = HandlingClass(should_fail=True)
    failing_consumer = RedisStreamConsumer(
        "input-stream", "consumer-id", failing_handler, redis_url
    )

    failing_consumer.start()
    time.sleep(1.5)  # allow one attempt and retry sleep
    failing_consumer.stop()

    # Output stream must be empty because the publish was rolled back
    client = Redis.from_url(redis_url)
    assert len(client.xrange("output-stream")) == 0


def test_inherited_persistable_attributes_restored_on_restart(redis_url):
    """
    Test that PersistableAttributes defined on a parent class are persisted and restored
    alongside those defined on the subclass.
    Process three messages with a two-level Persistable hierarchy, then restart with a fresh
    handler and one new message. Assert both the parent and child attributes are restored
    from Redis rather than reset to their defaults.
    """

    done = threading.Event()

    class BaseHandler(Persistable):
        base_count = PersistableAttribute(default=0)

        @property
        def unique_id(self) -> str:
            return "handler-main"

    class ChildHandler(BaseHandler):
        child_count = PersistableAttribute(default=0)

        def handle(self, message: dict[str, Any], transaction: TransactionManager) -> None:
            self.base_count += 1
            self.child_count += 1
            if self.base_count >= 3:
                done.set()

    for i in range(3):
        _publish(redis_url, "stream-id", {"id": f"{i}"})

    handler = ChildHandler()
    consumer = RedisStreamConsumer("stream-id", "consumer-id", handler, redis_url)
    _run_consumer(consumer, done)

    assert handler.base_count == 3
    assert handler.child_count == 3

    # Simulate crash: publish one more message and restart with a fresh handler instance
    _publish(redis_url, "stream-id", {"id": "3"})
    done.clear()

    fresh_handler = ChildHandler()
    restarted = RedisStreamConsumer("stream-id", "consumer-id", fresh_handler, redis_url)
    _run_consumer(restarted, done)

    # Both attributes restored (3) + 1 new message = 4. Without the MRO fix, base_count
    # would not be persisted and would reset to 1 after restart.
    assert fresh_handler.base_count == 4
    assert fresh_handler.child_count == 4


def test_message_redelivered_after_failed_callback(redis_url):
    """
    Test that a failed handler callback does not advance the consumer position.
    Publish one message and run a handler that raises on the first attempt.
    Assert the message is redelivered and successfully processed on the next attempt.
    """

    class HandlingClass(Persistable):
        count = PersistableAttribute(default=0)

        @property
        def unique_id(self) -> str:
            return "handler-main"

        def handle(self, message: dict[str, Any], transaction: TransactionManager) -> None:
            self.count += 1

    _publish(redis_url, "stream-id", {"id": "1"})

    attempt = [0]
    done = threading.Event()

    handler = HandlingClass()
    original = handler.handle

    def flaky_handle(msg, tx):
        attempt[0] += 1
        if attempt[0] == 1:
            raise RuntimeError("Transient failure on first attempt")
        original(msg, tx)
        done.set()

    handler.handle = flaky_handle

    consumer = RedisStreamConsumer("stream-id", "consumer-id", handler, redis_url)
    _run_consumer(consumer, done, timeout=6.0)

    # Message was attempted twice: failed once, succeeded once
    assert attempt[0] == 2
    # State reflects exactly one successful completion
    assert handler.count == 1
