"""
Simulate CDC changes in PostgreSQL for demo/testing.
Periodically inserts, updates, and deletes rows to trigger Debezium events.
"""

import os
import random
import time

import psycopg2

from configs.logging_config import setup_logging

logger = setup_logging(__name__)

INTERVAL_SECONDS = int(os.getenv("CDC_SIM_INTERVAL", "30"))


def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "ecommerce"),
        user=os.getenv("POSTGRES_USER", "ecommerce_user"),
        password=os.getenv("POSTGRES_PASSWORD", "ecommerce_pass"),
    )


def simulate_insert(cur) -> None:
    cur.execute(
        """
        INSERT INTO ecommerce_cdc.customers (email, first_name, last_name, country, city)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING customer_id
        """,
        (
            f"user_{random.randint(10000, 99999)}@example.com",
            f"User{random.randint(1, 999)}",
            "Test",
            random.choice(["US", "UK", "CA"]),
            "Demo City",
        ),
    )
    customer_id = cur.fetchone()[0]
    logger.info("CDC INSERT: customer_id=%s", customer_id)


def simulate_update(cur) -> None:
    cur.execute(
        """
        UPDATE ecommerce_cdc.orders
        SET order_status = %s, updated_at = NOW()
        WHERE order_id = (
            SELECT order_id FROM ecommerce_cdc.orders
            ORDER BY RANDOM() LIMIT 1
        )
        RETURNING order_id, order_status
        """,
        (random.choice(["shipped", "delivered", "cancelled"]),),
    )
    row = cur.fetchone()
    if row:
        logger.info("CDC UPDATE: order_id=%s status=%s", row[0], row[1])


def simulate_inventory_change(cur) -> None:
    delta = random.randint(-5, 20)
    cur.execute(
        """
        UPDATE ecommerce_cdc.products
        SET stock_quantity = GREATEST(0, stock_quantity + %s),
            updated_at = NOW()
        WHERE product_id = (
            SELECT product_id FROM ecommerce_cdc.products
            ORDER BY RANDOM() LIMIT 1
        )
        RETURNING product_id, stock_quantity
        """,
        (delta,),
    )
    row = cur.fetchone()
    if row:
        cur.execute(
            """
            INSERT INTO ecommerce_cdc.inventory_log
                (product_id, change_type, quantity_delta, previous_stock, new_stock)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (row[0], "adjustment", delta, row[1] - delta, row[1]),
        )
        logger.info("CDC INVENTORY: product_id=%s stock=%s", row[0], row[1])


def main() -> None:
    logger.info("Starting CDC change simulator (interval=%ds)", INTERVAL_SECONDS)
    actions = [simulate_insert, simulate_update, simulate_inventory_change]

    while True:
        try:
            with get_connection() as conn:
                conn.autocommit = True
                with conn.cursor() as cur:
                    random.choice(actions)(cur)
        except Exception as e:
            logger.error("CDC simulation error: %s", e)
        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
