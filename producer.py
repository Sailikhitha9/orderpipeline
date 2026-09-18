
import csv
import json
import logging
from kafka import KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info("Starting Kafka producer")

producer = None

try:
    producer = KafkaProducer(
        bootstrap_servers="localhost:7001",
        value_serializer=lambda data: json.dumps(data).encode("utf-8")
    )

    logging.info("Connected to Kafka")

    with open("orders.csv", "r", newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        count = 0

        for order in reader:
            producer.send("orders", value=order)
            count += 1
            logging.info(f"Published order: {order['order_id']}")

        producer.flush()

    logging.info(f"Finished publishing {count} orders")

except Exception as error:
    logging.error(f"Producer failed: {error}")

finally:
    if producer:
        producer.close()

