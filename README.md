# 🚀 Order Streaming Pipeline

---

## 🔄 Flow

**`orders.csv` → `Python Producer` → `Kafka` → `Flink` → `PostgreSQL`**

---

## 📁 Project Files

| 📄 File | 📝 Description |
|---|---|
| `docker-compose.yml` | Starts Kafka, Flink and PostgreSQL |
| `Dockerfile.flink` | Flink Docker image |
| `orders.csv` | Sample order data (100+ records) |
| `producer.py` | Sends orders to Kafka |
| `orderconsumer.py` | Flink job that processes orders |
| `requirements.txt` | Python dependencies |
| `postgres/init.sql` | Creates the PostgreSQL table |

---

## ▶️ How to Run

### 1️⃣ Start Docker Services

Open **PowerShell** in the project folder and run:

```powershell
docker compose up -d --build
🔍 Check the Containers
docker compose ps
2️⃣ Run the Kafka Producer

From the project folder, run:

python producer.py

The producer reads orders.csv and sends the orders to the Kafka orders topic.

3️⃣ Start the Flink Job

Run:

docker exec order-flink-jobmanager flink run /opt/flink/jobs/orderconsumer.py
🔍 Check the Flink Job
docker exec order-flink-jobmanager flink list
🔎 Check the Results
⚡ Check Flink Logs
docker logs order-flink-taskmanager
🐘 Check PostgreSQL Record Count
docker exec order-postgres psql -U orders -d orders_db -c "SELECT COUNT(*) FROM processed_orders;"
📊 View Processed Orders
docker exec order-postgres psql -U orders -d orders_db -c "SELECT order_id, customer_name, product, amount, order_category FROM processed_orders LIMIT 10;"
🎯 Expected Result

All orders from orders.csv are:

📄 Published to Kafka
↓
⚡ Processed by Flink
↓
🏷️ Classified as HIGH_VALUE or NORMAL
↓
🐘 Stored in PostgreSQL

✅ Pipeline Complete
📄 orders.csv
      ↓
🐍 Python Producer
      ↓
📨 Kafka
      ↓
⚡ Flink
      ↓
🐘 PostgreSQL

🎉 End-to-end order streaming pipeline completed successfully!
