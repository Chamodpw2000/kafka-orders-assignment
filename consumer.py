from confluent_kafka import DeserializingConsumer
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

BOOTSTRAP = "localhost:9092"
SCHEMA_REGISTRY = "http://localhost:8081"
TOPIC = "orders"
GROUP_ID = "order-processor"


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

            total += order["price"]
            count += 1
            average = total / count

            print(
                f"offset={msg.offset():<4} "
                f"order={order['orderId']} "
                f"{order['product']:<28} "
                f"price={order['price']:>7.2f} | "
                f"count={count:<4} avg={average:.2f}"
            )

            consumer.commit(msg)

    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
