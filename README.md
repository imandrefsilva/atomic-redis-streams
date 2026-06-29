# atomic-redis-streams

`atomic-streams` is a small Python library that adds crash-safe, stateful processing on top of Redis Streams.

Raw Redis Streams give you persistence and at-least-once delivery but do not give:
- Atomic application state that survives crashes
- Exactly-once processing (no duplicate messages after restart)
- Downstream message publications that only happen if the callback succeeded

This library fills that gap. It wraps Redis Streams consumers in a transaction model where the callback result, application state, consumer position, and any outgoing publishes all commit together or not at all.

## Installation

```bash
pip install atomic-redis-streams
```

## Examples

The core concept is a **handler**: a class with a `handle()` method that the library calls for each message. You point a `RedisStreamConsumer` at a Redis stream, give it a handler, and it takes care of the rest.

### Stateless consumer

The simplest handler just implements `handle()`. Use this when you don't need to track any state between messages:

```python
from atomic_redis_streams import RedisStreamConsumer, TransactionManager

class Logger:
    def handle(self, message: dict, transaction: TransactionManager) -> None:
        print(f"Received: {message}")

consumer = RedisStreamConsumer(
    stream="events",
    consumer_id="logger",
    handler=Logger(),
    redis_url="redis://localhost:6379",
)
consumer.start()
```

### Stateful consumer with crash recovery

Extend `Persistable` to persist state atomically with each message. On restart, state is restored and no message is processed twice:

```python
from atomic_redis_streams import Persistable, PersistableAttribute, RedisStreamConsumer, TransactionManager

class OrderCounter(Persistable):
    count = PersistableAttribute(default=0)

    @property
    def unique_id(self) -> str:
        return "order-counter"

    def handle(self, message: dict, transaction: TransactionManager) -> None:
        self.count += 1
        print(f"Processed order {message['order_id']} (total: {self.count})")

consumer = RedisStreamConsumer(
    stream="orders",
    consumer_id="order-processor",
    handler=OrderCounter(),
    redis_url="redis://localhost:6379",
)
consumer.start()
```

### Atomic downstream publishing

Use `transaction.publish()` to forward messages to another stream. The publish is committed atomically with the handler, so if the handler raises, the publish is rolled back:

```python
from atomic_redis_streams import RedisStreamConsumer, TransactionManager

class OrderRouter:
    def handle(self, message: dict, transaction: TransactionManager) -> None:
        transaction.publish("invoices", {"order_id": message["order_id"]})
        # if an exception is raised here, the above publish() call is rolled back

consumer = RedisStreamConsumer(
    stream="orders",
    consumer_id="order-router",
    handler=OrderRouter(),
    redis_url="redis://localhost:6379",
)
consumer.start()
```

### Full example

This example combines all three features: reading a message, updating persistent state, and publishing to a downstream stream. All three happen atomically:

```python
from atomic_redis_streams import Persistable, PersistableAttribute, RedisStreamConsumer, TransactionManager

RESTOCK_THRESHOLD = 10

class InventoryTracker(Persistable):
    stock = PersistableAttribute(default=0)

    @property
    def unique_id(self) -> str:
        return "inventory-tracker"

    def handle(self, message: dict, transaction: TransactionManager) -> None:
        self.stock -= int(message["quantity"])
        if self.stock <= RESTOCK_THRESHOLD:
            transaction.publish("restock-alerts", {
                "sku": message["sku"],
                "stock": str(self.stock),
            })

consumer = RedisStreamConsumer(
    stream="sales",
    consumer_id="inventory-tracker",
    handler=InventoryTracker(),
    redis_url="redis://localhost:6379",
)
consumer.start()
```

If the process crashes mid-flight, the stock count is not updated and the restock alert is not published. On restart, processing resumes from the last successfully committed message.

## Security

This library uses `pickle` to serialize and deserialize `Persistable` handler state in Redis. **It assumes a trusted Redis connection.** If an attacker can write arbitrary bytes to your Redis instance, they can execute arbitrary code when the state is restored on startup. Do not use this library against an untrusted or publicly accessible Redis instance.
