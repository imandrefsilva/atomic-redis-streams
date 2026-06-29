import logging
import threading
from typing import Any, Protocol

from redis import Redis

from .exceptions import SchemaMismatchException
from .persistable import _SCHEMA_HASH_FIELD, Persistable
from .transaction import TransactionManager

logger = logging.getLogger(__name__)

BLOCK_MS = 100


class MessageHandler(Protocol):
    def handle(self, message: dict[str, Any], transaction: TransactionManager) -> None: ...


class RedisStreamConsumer:
    """
    A crash-safe, stateful Redis Streams consumer.

    On each message:
      1. Runs the handler callback
      2. Persists handler state (if handler is Persistable)
      3. Advances the consumer position in the stream
      4. Commits all three atomically via TransactionManager

    On restart, restores handler state from Redis and resumes from the last
    successfully processed message position.
    """

    def __init__(
        self,
        stream: str,
        consumer_id: str,
        handler: MessageHandler,
        redis_url: str = "redis://localhost:6379",
    ) -> None:
        """
        Args:
            stream: Redis stream key to consume from.
            consumer_id: Unique identifier for this consumer instance; used to
                track its position in the stream and (if the handler is
                Persistable) to namespace its persisted state in Redis.
            handler: Callback that processes each message. Must implement
                ``MessageHandler.handle``; optionally implements ``Persistable``
                so its state is snapshotted and restored across restarts.
            redis_url: Connection URL passed to ``Redis.from_url``.
        """

        # Redis stream key to consume from
        self._stream = stream
        # stable identifier for this consumer, used to track its position and namespace its state
        self._consumer_id = consumer_id
        # message handler called for each incoming message
        self._handler = handler
        # Redis client used for all stream and state operations
        self._redis: Redis = Redis.from_url(redis_url, decode_responses=False)
        # signals the consumer loop to stop; set() by stop(), checked by _run()
        self._stop_event = threading.Event()
        # background thread running the consumer loop
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()

        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None

    def _run(self) -> None:
        self._restore_handler_state()
        last_id = self._get_last_processed_id()

        logger.info(
            "Consumer %s starting from %s on stream %s", self._consumer_id, last_id, self._stream
        )

        needs_resync = False

        while not self._stop_event.is_set():
            if needs_resync:
                try:
                    new_last_id = self._get_last_processed_id()
                    self._restore_handler_state()
                    last_id = new_last_id
                    needs_resync = False
                except Exception:
                    logger.exception("Failed to resync state from Redis; will retry")
                    self._stop_event.wait(1.0)
                    continue

            try:
                results = self._redis.xread({self._stream: last_id}, count=1, block=BLOCK_MS)
                if not results:
                    continue
                _, messages = results[0]
                for message_id, fields in messages:
                    self._process_message(message_id, fields)
                    last_id = message_id
            except Exception:
                logger.exception("Error processing message, retrying in 1s")
                needs_resync = True
                self._stop_event.wait(1.0)

    def _index_key(self) -> str:
        return f"atomic_streams:index:{self._stream}:{self._consumer_id}"

    def _get_last_processed_id(self) -> bytes:
        result = self._redis.get(self._index_key())
        return result if result is not None else b"0-0"

    def _restore_handler_state(self) -> None:
        if not isinstance(self._handler, Persistable):
            return

        key = f"atomic_streams:state:{self._handler.unique_id}"
        raw = self._redis.hgetall(key)

        if not raw:
            return

        stored_hash = raw.pop(_SCHEMA_HASH_FIELD.encode(), None)

        if stored_hash is not None and stored_hash.decode() != self._handler._get_class_hash():
            raise SchemaMismatchException(
                f"Persisted schema for '{self._handler.unique_id}' does not match the current "
                f"class definition. Add or remove of PersistableAttribute fields requires "
                f"clearing or migrating the persisted state before restarting."
            )

        self._handler._restore_from_values({k.decode(): v for k, v in raw.items()})

    def _process_message(self, message_id: bytes, fields: dict[bytes, bytes]) -> None:
        parsed = {k.decode(): v.decode() for k, v in fields.items()}
        tm = TransactionManager(self._redis)
        with tm:
            self._handler.handle(parsed, tm)
            if isinstance(self._handler, Persistable):
                tm._persist_attributes(self._handler)
            tm._advance_consumer_index(self._consumer_id, self._stream, message_id)
