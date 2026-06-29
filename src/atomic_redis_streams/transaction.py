from typing import Any, Self

from redis import Redis
from redis.client import Pipeline

from .exceptions import NestedTransactionException, UninitializedTransactionException
from .persistable import _SCHEMA_HASH_FIELD, Persistable

# Maximum number of entries kept in a stream before Redis trims older ones
STREAM_MAX_LEN = 10_000


class TransactionManager:
    """
    Stages Redis operations into a single atomic pipeline.

    All operations (state persistence, consumer index advancement, downstream
    publishes) are buffered and written together on commit(). If the process
    crashes before commit(), nothing is written.
    """

    def __init__(self, redis: Redis) -> None:
        # Redis client used to create pipelines
        self._redis = redis
        # active pipeline; None when no transaction is open
        self._pipeline: Pipeline | None = None

    @property
    def is_open(self) -> bool:
        return self._pipeline is not None

    def begin(self) -> None:
        if self.is_open:
            raise NestedTransactionException()

        self._pipeline = self._redis.pipeline(transaction=True)

    def abort(self) -> None:
        self._pipeline = None

    def commit(self) -> None:
        if not self.is_open:
            raise UninitializedTransactionException()

        self._pipeline.execute()
        self._pipeline = None

    def publish(self, stream: str, fields: dict[str, str]) -> None:
        """
        Stage a message to be published to ``stream`` when this transaction commits.

        All field values must be strings. Redis Streams store field values as strings;
        non-string types (int, bool, list, …) must be serialized by the caller before
        passing them here.

        The publish is rolled back if the transaction is aborted (e.g. the handler raises).
        """
        if not self.is_open:
            raise UninitializedTransactionException()

        self._pipeline.xadd(stream, fields, maxlen=STREAM_MAX_LEN, approximate=True)

    def _persist_attributes(self, *instances: Persistable) -> None:
        if not self.is_open:
            raise UninitializedTransactionException()

        for instance in instances:
            key = f"atomic_streams:state:{instance.unique_id}"
            mapping: dict[str, bytes] = instance._get_persistable_attributes_values()
            mapping[_SCHEMA_HASH_FIELD] = instance._get_class_hash().encode()
            self._pipeline.hset(key, mapping=mapping)

    def _advance_consumer_index(self, consumer_id: str, stream: str, message_id: bytes) -> None:
        if not self.is_open:
            raise UninitializedTransactionException()

        key = f"atomic_streams:index:{stream}:{consumer_id}"
        self._pipeline.set(key, message_id)

    def __enter__(self) -> Self:
        self.begin()

        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if exc_type is None and self.is_open:
            try:
                self.commit()
            except Exception:
                self.abort()
                raise

        elif self.is_open:
            self.abort()
