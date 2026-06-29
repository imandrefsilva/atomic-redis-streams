class AtomicStreamsException(Exception):
    """
    Base class for all atomic-redis-streams exceptions.
    """


class UninitializedTransactionException(AtomicStreamsException):
    """
    Raised when a TransactionManager operation is called before begin().
    """


class NestedTransactionException(AtomicStreamsException):
    """
    Raised when begin() is called on a TransactionManager that already has an open transaction.

    Nested transactions are not supported; each transaction must be committed or aborted
    before a new one can be started.
    """


class SchemaMismatchException(AtomicStreamsException):
    """
    Raised when the persisted schema hash for a Persistable handler does not match the current
    class definition.

    This happens when PersistableAttribute fields are added or removed between runs. The persisted
    state cannot be safely loaded and must be cleared or migrated before restarting.
    """
