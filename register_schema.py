import json
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema

client = SchemaRegistryClient({"url": "http://localhost:8081"})

with open("order.avsc") as f:
    schema_str = f.read()

schema_id = client.register_schema("orders-value", Schema(schema_str, "AVRO"))
print("Registered with schema ID:", schema_id)