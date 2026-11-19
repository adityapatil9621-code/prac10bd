
import json, time
from datetime import datetime
import clickhouse_connect
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

client = clickhouse_connect.get_client(
    host="clickhouse",
    port=8123,
    username="default",
    password="prac123"
)

client.command("""
CREATE TABLE IF NOT EXISTS default.air_quality (
    event_time DateTime,
    city String,
    pm25 Float64,
    pm10 Float64,
    temperature Float64
)
ENGINE = MergeTree()
ORDER BY event_time
""")

while True:
    try:
        consumer = KafkaConsumer(
            "air_quality",
            bootstrap_servers="kafka:9092",
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            group_id="air-quality-group",
            auto_offset_reset="earliest"
        )
        break
    except NoBrokersAvailable:
        print("Waiting for Kafka...")
        time.sleep(3)

print("Consumer Started")

try:
    for msg in consumer:
        d = msg.value

        d["event_time"] = datetime.strptime(
            d["event_time"], "%Y-%m-%d %H:%M:%S"
        )

        client.insert(
            "default.air_quality",
            [[d["event_time"], d["city"], d["pm25"],
              d["pm10"], d["temperature"]]],
            column_names=[
                "event_time", "city", "pm25",
                "pm10", "temperature"
            ]
        )

        print("Inserted:", d)

except KeyboardInterrupt:
    print("Consumer stopped")

finally:
    consumer.close()
    client.close()
