"""
Realistic e-commerce event generators — portfolio/demo scale.

At 15 events/sec ≈ 1.3M events/day (documented as 100K+ capable).
Weighted mix simulates typical e-commerce funnel: clicks > orders > payments.
"""

import random
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List

from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

PRODUCTS: List[Dict[str, Any]] = [
    {"product_id": "SKU-001", "name": "Wireless Headphones", "category": "Electronics", "price": 79.99},
    {"product_id": "SKU-002", "name": "Running Shoes", "category": "Footwear", "price": 129.99},
    {"product_id": "SKU-003", "name": "Coffee Maker", "category": "Home", "price": 49.99},
    {"product_id": "SKU-004", "name": "Laptop Stand", "category": "Electronics", "price": 34.99},
    {"product_id": "SKU-005", "name": "Yoga Mat", "category": "Sports", "price": 29.99},
    {"product_id": "SKU-006", "name": "Smart Watch", "category": "Electronics", "price": 199.99},
    {"product_id": "SKU-007", "name": "Backpack", "category": "Accessories", "price": 59.99},
    {"product_id": "SKU-008", "name": "Desk Lamp", "category": "Home", "price": 39.99},
    {"product_id": "SKU-009", "name": "Mechanical Keyboard", "category": "Electronics", "price": 149.99},
    {"product_id": "SKU-010", "name": "Water Bottle", "category": "Sports", "price": 24.99},
    {"product_id": "SKU-011", "name": "Blender", "category": "Home", "price": 89.99},
    {"product_id": "SKU-012", "name": "Sunglasses", "category": "Accessories", "price": 45.00},
]

# Weighted geography — realistic traffic skew
COUNTRIES = ["US", "US", "US", "UK", "CA", "DE", "FR", "AU", "JP", "IN", "BR", "MX"]
CITIES = {
    "US": ["New York", "San Francisco", "Chicago", "Austin"],
    "UK": ["London", "Manchester"],
    "CA": ["Toronto", "Vancouver"],
    "DE": ["Berlin", "Munich"],
    "FR": ["Paris", "Lyon"],
    "AU": ["Sydney", "Melbourne"],
    "JP": ["Tokyo", "Osaka"],
    "IN": ["Mumbai", "Bangalore"],
    "BR": ["São Paulo"],
    "MX": ["Mexico City"],
}

PAGES = ["/", "/products", "/products", "/cart", "/checkout", "/search", "/account", "/deals"]
PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "apple_pay", "google_pay"]
ORDER_STATUSES = ["pending", "confirmed", "shipped", "delivered", "cancelled", "returned"]
DEVICES = ["mobile", "mobile", "desktop", "tablet"]


def _timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def _user_id() -> str:
    return f"user_{random.randint(1000, 99999)}"


def _location() -> tuple:
    country = random.choice(COUNTRIES)
    city = random.choice(CITIES.get(country, [fake.city()]))
    return country, city


def generate_order_event() -> Dict[str, Any]:
    product = random.choice(PRODUCTS)
    quantity = random.choices([1, 2, 3], weights=[70, 20, 10])[0]
    unit_price = round(product["price"] * random.uniform(0.95, 1.05), 2)
    country, city = _location()

    return {
        "event_type": "order_placed",
        "order_id": str(uuid.uuid4()),
        "user_id": _user_id(),
        "session_id": str(uuid.uuid4()),
        "product_id": product["product_id"],
        "product_name": product["name"],
        "category": product["category"],
        "quantity": quantity,
        "unit_price": unit_price,
        "total_amount": round(unit_price * quantity, 2),
        "currency": "USD",
        "country": country,
        "city": city,
        "order_status": random.choice(ORDER_STATUSES),
        "timestamp": _timestamp(),
    }


def generate_payment_event(order_id: str | None = None) -> Dict[str, Any]:
    amount = round(random.uniform(15.0, 650.0), 2)
    status = random.choices(["success", "failed", "pending"], weights=[0.88, 0.08, 0.04])[0]
    country, _ = _location()

    return {
        "event_type": "payment_processed",
        "payment_id": str(uuid.uuid4()),
        "order_id": order_id or str(uuid.uuid4()),
        "user_id": _user_id(),
        "amount": amount,
        "currency": "USD",
        "payment_method": random.choice(PAYMENT_METHODS),
        "status": status,
        "failure_reason": random.choice(["insufficient_funds", "card_declined", "timeout"]) if status == "failed" else None,
        "country": country,
        "timestamp": _timestamp(),
    }


def generate_click_event() -> Dict[str, Any]:
    country, city = _location()
    return {
        "event_type": "page_view",
        "event_id": str(uuid.uuid4()),
        "session_id": str(uuid.uuid4()),
        "user_id": _user_id(),
        "page_url": random.choice(PAGES),
        "referrer": random.choice(["google", "direct", "email", "social", None]),
        "device": random.choice(DEVICES),
        "country": country,
        "city": city,
        "duration_seconds": random.randint(2, 420),
        "timestamp": _timestamp(),
    }


def generate_user_activity_event() -> Dict[str, Any]:
    country, city = _location()
    return {
        "event_type": "user_activity",
        "activity_id": str(uuid.uuid4()),
        "user_id": _user_id(),
        "session_id": str(uuid.uuid4()),
        "activity_type": random.choice(
            ["login", "logout", "add_to_cart", "remove_from_cart", "wishlist_add", "profile_update"]
        ),
        "country": country,
        "city": city,
        "device": random.choice(DEVICES),
        "timestamp": _timestamp(),
    }


def generate_inventory_update_event() -> Dict[str, Any]:
    product = random.choice(PRODUCTS)
    delta = random.randint(-8, 40)
    previous = random.randint(50, 800)
    return {
        "event_type": "inventory_update",
        "update_id": str(uuid.uuid4()),
        "product_id": product["product_id"],
        "product_name": product["name"],
        "category": product["category"],
        "change_type": "restock" if delta > 0 else "sale",
        "quantity_delta": delta,
        "previous_stock": previous,
        "new_stock": max(0, previous + delta),
        "warehouse_id": f"WH-{random.randint(1, 8)}",
        "timestamp": _timestamp(),
    }


def generate_return_event(order_id: str | None = None) -> Dict[str, Any]:
    product = random.choice(PRODUCTS)
    qty = random.randint(1, 2)
    country, city = _location()
    return {
        "event_type": "order_returned",
        "return_id": str(uuid.uuid4()),
        "order_id": order_id or str(uuid.uuid4()),
        "user_id": _user_id(),
        "product_id": product["product_id"],
        "product_name": product["name"],
        "quantity": qty,
        "refund_amount": round(product["price"] * qty, 2),
        "reason": random.choice(["defective", "wrong_item", "changed_mind", "late_delivery"]),
        "country": country,
        "city": city,
        "timestamp": _timestamp(),
    }
