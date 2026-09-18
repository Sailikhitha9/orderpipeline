import json
import logging
import time
import psycopg2

from pyflink.common import SimpleStringSchema, WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.functions import MapFunction


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


HIGH_VALUE_THRESHOLD = 10000


class OrderProcessor(MapFunction):

    def map(self, value):
        order_id = "UNKNOWN"
        conn = None

        try:
            # Convert Kafka JSON string into Python dictionary
            order = json.loads(value)
            order_id = order["order_id"]

            logging.info(f"Flink received order {order_id}")

            # Convert amount to number
            amount = float(order["amount"])

            # Classify the order
            if amount >= HIGH_VALUE_THRESHOLD:
                category = "HIGH_VALUE"
            else:
                category = "NORMAL"

            logging.info(
                f"Order {order_id} classified as {category}"
            )

            # Connect to PostgreSQL
            time.sleep(1)

            conn = psycopg2.connect(
                host="postgres",
                port="5432",
                dbname="orders_db",
                user="orders",
                password="orders"
            )

            conn.autocommit = True

            logging.info("Connected to PostgreSQL")

            # Insert processed order
            cur = conn.cursor()

            cur.execute(
                """
                INSERT INTO processed_orders
                (
                    order_id,
                    customer_name,
                    product,
                    amount,
                    order_category
                )
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    order_id,
                    order["customer_name"],
                    order["product"],
                    amount,
                    category
                )
            )

            cur.close()

            logging.info(
                f"Order {order_id} stored in PostgreSQL"
            )

        except Exception as e:

            logging.error(
                f"Failed to process order: {order_id} - {e}"
            )

        finally:

            if conn:
                conn.close()

        return order_id


def main():

    # Create Flink execution environment
    env = StreamExecutionEnvironment.get_execution_environment()

    env.set_parallelism(1)

    # Create Kafka source
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("kafka:9092")
        .set_topics("orders")
        .set_group_id("flink-order-consumer")
        .set_starting_offsets(
            KafkaOffsetsInitializer.earliest()
        )
        .set_value_only_deserializer(
            SimpleStringSchema()
        )
        .build()
    )

    # No watermarks needed for this assignment
    watermark = WatermarkStrategy.no_watermarks()

    # Read orders from Kafka
    stream = env.from_source(
        source,
        watermark,
        "kafka-source"
    )

    # Process each order
    processed = stream.map(
        OrderProcessor(),
        output_type=Types.STRING()
    )

    # Show processed order IDs in Flink logs
    processed.print()

    logging.info(
        "Starting Flink job to consume orders"
    )

    # Start the Flink job
    env.execute("order-processing-job")


main()