CREATE TABLE IF NOT EXISTS processed_orders (
    id SERIAL PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL,
    customer_name VARCHAR(100),
    product VARCHAR(100),
    amount DECIMAL(10,2),
    order_category VARCHAR(20),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);