import random
import time

from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

BOOTSTRAP = "localhost:9092"
SCHEMA_REGISTRY = "http://localhost:8081"
TOPIC = "orders"

from catalog import PRODUCTS

def delivery_report(err, msg):
    if err is not None:
        print(f"[FAILED] {err}")
    else:
        print(f"[OK] offset {msg.offset()} -> {msg.topic()}")


def main():
    with open("order.avsc") as f:
        schema_str = f.read()

    sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY})
    avro_serializer = AvroSerializer(sr_client, schema_str)

    producer = SerializingProducer({
        "bootstrap.servers": BOOTSTRAP,
        "key.serializer": StringSerializer("utf_8"),
        "value.serializer": avro_serializer,
        "acks": "all",
    })

    order_id = 1000

    try:
        while True:
            order_id += 1
            item = random.choice(PRODUCTS)
            order = {
                "orderId": str(order_id),
                "product": item["product"],
                "price": item["price"],
            }

            producer.produce(
                topic=TOPIC,
                key=order["orderId"],
                value=order,
                on_delivery=delivery_report,
            )

            print(f"Sent: {order}")
            producer.poll(0)
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
