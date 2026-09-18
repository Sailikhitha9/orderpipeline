Flow

orders.csv → Python Producer → Kafka → Flink → PostgreSQL

Project Files

docker-compose.yml - Starts Kafka, Flink and PostgreSQL

Dockerfile.flink - Flink Docker image

orders.csv - Sample order data (100+ records)

producer.py - Sends orders to Kafka

orderconsumer.py - Flink job that processes orders

requirements.txt - Python dependencies

postgres/init.sql - Creates the PostgreSQL table
How to Run

1. Start Docker services

Open PowerShell in the project folder and run:

docker compose up -d --build

Check the containers:

docker compose ps
 Run the Kafka producer

From the project folder:

python producer.py

The producer reads orders.csv and sends the orders to the Kafka orders topic.
. Start the Flink job

docker exec order-flink-jobmanager flink run /opt/flink/jobs/orderconsumer.py

Check the job:

docker exec order-flink-jobmanager flink list
Check the Results

Check Flink logs

docker logs order-flink-taskmanager

Check PostgreSQL record count

docker exec order-postgres psql -U orders -d orders_db -c "SELECT COUNT(*) FROM processed_orders;"

View processed orders

docker exec order-postgres psql -U orders -d orders_db -c "SELECT order_id, customer_name, product, amount, order_category FROM processed_orders LIMIT 10;"

Expected Result

All orders from orders.csv are published to Kafka, processed by Flink, classified as HIGH_VALUE or NORMAL, and stored in PostgreSQL.
