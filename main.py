import random
from faker import Faker
import psycopg2
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import os

# Load environment variables from a `.env` file.
load_dotenv()

# Faker configuration
fake = Faker('pt_BR')
Faker.seed(42)

# PostgreSQL connection configuration
conn = psycopg2.connect(
    host=os.getenv("HOST"),
    database=os.getenv("DATABASE"),
    user=os.getenv("USER"),
    password=os.getenv("PASSWORD")
)
cur = conn.cursor()

# Function to create tables with TIMESTAMP in UTC
def create_tables():
    commands = [
        """
        CREATE TABLE IF NOT EXISTS products (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            created_at TIMESTAMPTZ NOT NULL,
            price NUMERIC(10, 2) NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS customers (
            id SERIAL PRIMARY KEY,
            created_at TIMESTAMPTZ NOT NULL,
            first_name VARCHAR(50) NOT NULL,
            last_name VARCHAR(50) NOT NULL,
            email VARCHAR(100) NOT NULL UNIQUE
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS refunds (
            id SERIAL PRIMARY KEY,
            created_at TIMESTAMPTZ NOT NULL,
            reason VARCHAR(100) NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS order_line_items (
            id SERIAL PRIMARY KEY,
            order_id INTEGER NOT NULL,
            product_id INTEGER REFERENCES products(id),
            customer_id INTEGER REFERENCES customers(id),
            refund_id INTEGER REFERENCES refunds(id),
            created_at TIMESTAMPTZ NOT NULL,
            quantity INTEGER NOT NULL
        )
        """
    ]
    for command in commands:
        cur.execute(command)
    conn.commit()
    print("Tables created successfully.")

# 1. Insert data into products table
def insert_products():
    products = [
        ("Café Turbinado do Zé", datetime(2023, 1, 1, tzinfo=timezone.utc), 12.99),
        ("Expresso dos Sonhos", datetime(2023, 1, 1, tzinfo=timezone.utc), 9.99),
        ("Latte Encantado", datetime(2023, 1, 1, tzinfo=timezone.utc), 14.99),
        ("Capuccino Surpresa", datetime(2023, 1, 1, tzinfo=timezone.utc), 13.99)
    ]
    cur.executemany("INSERT INTO products (name, created_at, price) VALUES (%s, %s, %s)", products)
    conn.commit()
    print("Products inserted successfully.")

# 2. Insert data into customers table
def insert_customers():
    num_customers = 200
    customers = []
    start_date_customers = datetime(2023, 1, 1, tzinfo=timezone.utc)
    
    for _ in range(num_customers):
        created_at = start_date_customers + timedelta(days=random.randint(0, 545))
        first_name = fake.first_name()
        last_name = fake.last_name()
        email = fake.email()
        customers.append((created_at, first_name, last_name, email))

    cur.executemany(
        "INSERT INTO customers (created_at, first_name, last_name, email) VALUES (%s, %s, %s, %s)",
        customers
    )
    conn.commit()
    print("Customers inserted successfully.")

# 3. Insert data into refunds table with updated reasons
def insert_refunds():
    reasons = [
        "Produto danificado", "Produto não condiz com a descrição",
        "Demora na entrega", "Problemas de qualidade"
    ]
    
    num_refunds = 100
    refunds = []
    for _ in range(num_refunds):
        created_at = datetime(2023, 1, 1, tzinfo=timezone.utc) + timedelta(days=random.randint(0, 545))
        reason = random.choice(reasons)
        refunds.append((created_at, reason))

    cur.executemany(
        "INSERT INTO refunds (created_at, reason) VALUES (%s, %s)",
        refunds
    )
    conn.commit()
    print("Refunds inserted successfully.")

# 4. Insert data into order_line_items table with approximate growth and refunds
def insert_order_line_items():
    start_date_orders = datetime(2023, 1, 1, tzinfo=timezone.utc)
    order_line_items = []
    order_id = 1
    
    # Growth in sales volume by 2-3% per month
    total_orders = 2000  # initial quantity for simulation
    current_date = start_date_orders

    while current_date <= datetime(2024, 6, 30, tzinfo=timezone.utc):
        for _ in range(random.randint(1, 10)):  # Number of orders per day
            customer_id = random.randint(1, 200)  # Assume 200 clients
            num_items = random.randint(1, 4)  # Between 1 and 4 SKUs per order
            for _ in range(num_items):
                product_id = random.randint(1, 4)
                quantity = random.randint(1, 3)
                
                # Approximately 5% of orders with refund
                refund_id = random.choice([None] * 19 + [random.randint(1, 100)])
                
                order_line_items.append(
                    (order_id, product_id, customer_id, refund_id, current_date, quantity)
                )
            order_id += 1
        current_date += timedelta(days=1)
    
    cur.executemany(
        """
        INSERT INTO order_line_items (order_id, product_id, customer_id, refund_id, created_at, quantity)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        order_line_items
    )
    conn.commit()
    print("Order line items inserted successfully.")

# Execute functions
create_tables()
insert_products()
insert_customers()
insert_refunds()
insert_order_line_items()

# Close connection
cur.close()
conn.close()
