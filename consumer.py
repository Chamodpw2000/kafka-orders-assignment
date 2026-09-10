from confluent_kafka import DeserializingConsumer
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
import random
import time




BOOTSTRAP = "localhost:9092"
SCHEMA_REGISTRY = "http://localhost:8081"
TOPIC = "orders"
GROUP_ID = "order-processor"


import random
import time


class TransientError(Exception):
    """Temporary failure — worth retrying."""


class PermanentError(Exception):
    """Invalid data — retrying will never help."""


MAX_RETRIES = 3
BASE_BACKOFF = 0.5
TRANSIENT_FAILURE_RATE = 0.2


def process_order(order):
    """Simulates downstream processing that can fail."""
    if order["price"] <= 0:
        raise PermanentError(f"Invalid price {order['price']}")

    if random.random() < TRANSIENT_FAILURE_RATE:
        raise TransientError("Downstream service temporarily unavailable")

    return order["price"]


def handle_with_retry(order):
    attempt = 0
    while True:
        try:
            return process_order(order)

        except PermanentError:
            raise                      # fail fast, no retries

        except TransientError as e:
            attempt += 1
            if attempt > MAX_RETRIES:
                raise
            backoff = BASE_BACKOFF * (2 ** (attempt - 1))
            print(f"      [retry {attempt}/{MAX_RETRIES}] {e} — waiting {backoff:.1f}s")
            time.sleep(backoff)

def main():
    with open("order.avsc") as f:
        schema_str = f.read()

    sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY})
    avro_deserializer = AvroDeserializer(sr_client, schema_str)

    consumer = DeserializingConsumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        "key.deserializer": StringDeserializer("utf_8"),
        "value.deserializer": avro_deserializer,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })

    consumer.subscribe([TOPIC])

    total = 0.0
    count = 0

    print("Consumer started. Waiting for messages...\n")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                print(f"[ERROR] {msg.error()}")
                continue

            order = msg.value()

            try:
                price = handle_with_retry(order)

                total += price
                count += 1
                average = total / count

                print(
                    f"offset={msg.offset():<4} "
                    f"order={order['orderId']} "
                    f"{order['product']:<28} "
                    f"price={price:>7.2f} | "
                    f"count={count:<4} avg={average:.2f}"
                )

            except PermanentError as e:
                print(f"offset={msg.offset():<4} [PERMANENT] {order['orderId']} — {e}")

            except TransientError as e:
                print(f"offset={msg.offset():<4} [EXHAUSTED] {order['orderId']} — {e}")

            consumer.commit(msg)

    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
