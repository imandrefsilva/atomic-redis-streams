import hashlib
import inspect
import pickle
from abc import ABC, abstractmethod
from typing import Any, ClassVar

# Reserved Redis hash field used to store and validate the schema hash alongside attributes.
# Not a valid Python identifier for a PersistableAttribute, so it cannot collide with user fields.
_SCHEMA_HASH_FIELD = "__schema_hash__"


class PersistableAttribute:
    """Descriptor for class attributes that are automatically persisted to Redis."""

    def __init__(self, default: Any = None) -> None:
        # value returned when the attribute has not been set on an instance
        self.default = default
        # attribute name on the owning class, populated by __set_name__
        self.name: str = ""

    def __set_name__(self, owner: type, name: str) -> None:
        self.name = name

    def __get__(self, obj: Any, objtype: type | None = None) -> Any:
        if obj is None:
            return self
        return obj.__dict__.get(f"_pa_{self.name}", self.default)

    def __set__(self, obj: Any, value: Any) -> None:
        obj.__dict__[f"_pa_{self.name}"] = value


class Persistable(ABC):
    """
    Base class for objects whose state survives process crashes.

    Subclasses declare PersistableAttribute fields. Those fields are serialized
    to Redis atomically alongside message processing and restored on startup.

    Subclasses must override unique_id with a stable, process-restart-safe string
    (e.g. a fixed name or a config-driven ID). It is used to namespace persisted
    state in Redis, so it must be the same value across restarts.
    """

    _persistable_attributes: ClassVar[dict[str, PersistableAttribute]] = {}

    @property
    @abstractmethod
    def unique_id(self) -> str: ...

    def _get_class_hash(self) -> str:
        key = f"{self.__class__.__name__}:{sorted(self._persistable_attributes.keys())}"
        return hashlib.md5(key.encode()).hexdigest()[:8]

    def _get_persistable_attributes_values(self) -> dict[str, bytes]:
        return {name: pickle.dumps(getattr(self, name)) for name in self._persistable_attributes}

    def _restore_from_values(self, values: dict[str, bytes]) -> None:
        for name, raw_value in values.items():
            if name in self._persistable_attributes:
                setattr(self, name, pickle.loads(raw_value))

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        cls._persistable_attributes = dict(
            inspect.getmembers(cls, lambda a: isinstance(a, PersistableAttribute))
        )
