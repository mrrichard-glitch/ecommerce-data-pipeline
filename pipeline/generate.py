"""Generate one day of realistic e-commerce source data for a Ghanaian online shop.

The generator stands in for the operational system (the "source"). It is
deterministic: the same run date always produces the same records, so any
pipeline run can be reproduced exactly.

About 3% of records are deliberately corrupted (duplicates, missing keys,
bad quantities, unknown products, non-numeric amounts, malformed emails) so
the data quality layer has real problems to catch.
"""
from __future__ import annotations

import csv
import hashlib
import random
import shutil
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path

CUSTOMER_POOL_SIZE = 600
DIRTY_RATE = 0.03

REGIONS: dict[str, list[str]] = {
    "Greater Accra": ["Accra", "Tema", "Madina", "Teshie", "Dansoman"],
    "Ashanti": ["Kumasi", "Obuasi", "Ejisu"],
    "Central": ["Cape Coast", "Kasoa", "Winneba"],
    "Western": ["Takoradi", "Tarkwa"],
    "Eastern": ["Koforidua", "Nkawkaw"],
    "Volta": ["Ho", "Hohoe"],
    "Northern": ["Tamale"],
}
REGION_WEIGHTS = [45, 20, 9, 9, 7, 5, 5]

FIRST_NAMES = [
    "Kwame", "Kofi", "Kwabena", "Yaw", "Kwaku", "Kojo", "Kwesi", "Ama", "Akosua",
    "Abena", "Efua", "Adwoa", "Yaa", "Esi", "Akua", "Selorm", "Elikem", "Mawuli",
    "Edem", "Dzifa", "Fatima", "Abdul", "Ibrahim", "Afia", "Nana", "Richard",
    "Grace", "Emmanuel", "Priscilla", "Daniel",
]
LAST_NAMES = [
    "Mensah", "Owusu", "Boateng", "Asante", "Osei", "Agyeman", "Appiah", "Darko",
    "Addo", "Quaye", "Tetteh", "Kugbadzor", "Agbeko", "Amenyo", "Dogbe", "Abubakar",
    "Mahama", "Ansah", "Frimpong", "Ofori", "Nkrumah", "Sarpong", "Acheampong",
]
EMAIL_DOMAINS = ["gmail.com", "yahoo.com", "outlook.com", "icloud.com"]
PHONE_PREFIXES = ["024", "054", "055", "059", "020", "050", "027", "026"]

# (category, product name, price in GHS)
CATALOG: list[tuple[str, str, str]] = [
    ("Phones & Tablets", "Tecno Spark 20", "1899.00"),
    ("Phones & Tablets", "Infinix Hot 40i", "1650.00"),
    ("Phones & Tablets", "Samsung Galaxy A15", "2450.00"),
    ("Phones & Tablets", "Oraimo 20,000mAh Power Bank", "289.00"),
    ("Phones & Tablets", "Fast Charger USB-C 25W", "95.00"),
    ("Electronics", "Bluetooth Speaker", "349.00"),
    ("Electronics", "32-inch Smart TV", "2199.00"),
    ("Electronics", "Wireless Earbuds", "259.00"),
    ("Electronics", "Rechargeable Standing Fan", "799.00"),
    ("Home & Kitchen", "Electric Kettle 1.7L", "189.00"),
    ("Home & Kitchen", "Rice Cooker 1.8L", "420.00"),
    ("Home & Kitchen", "Blender 1.5L", "365.00"),
    ("Home & Kitchen", "Non-stick Cookware Set", "540.00"),
    ("Fashion", "Kente Print Shirt", "180.00"),
    ("Fashion", "Ladies Ankara Dress", "245.00"),
    ("Fashion", "Leather Sandals", "150.00"),
    ("Fashion", "Sneakers", "399.00"),
    ("Beauty & Health", "Shea Butter 500g", "65.00"),
    ("Beauty & Health", "Black Soap Pack", "45.00"),
    ("Beauty & Health", "Body Lotion 400ml", "78.00"),
    ("Groceries", "Jasmine Rice 5kg", "155.00"),
    ("Groceries", "Cooking Oil 3L", "120.00"),
    ("Groceries", "Milo Tin 400g", "68.00"),
    ("Groceries", "Gari 2kg", "35.00"),
]

PAYMENT_METHODS = ["MTN MoMo", "Telecel Cash", "AirtelTigo Money", "Card", "Cash on Delivery"]
PAYMENT_WEIGHTS = [50, 12, 8, 15, 15]
ORDER_STATUSES = ["completed", "pending", "cancelled", "returned"]
STATUS_WEIGHTS = [80, 8, 8, 4]
PAYMENT_STATUS_FOR_ORDER = {
    "completed": "success",
    "pending": "pending",
    "cancelled": "failed",
    "returned": "refunded",
}

COLUMNS: dict[str, list[str]] = {
    "customers": ["customer_id", "full_name", "email", "phone", "region", "city", "signup_date"],
    "products": ["product_id", "product_name", "category", "unit_price"],
    "orders": ["order_id", "customer_id", "order_ts", "status", "delivery_fee"],
    "order_items": ["order_id", "line_number", "product_id", "quantity", "unit_price"],
    "payments": ["payment_id", "order_id", "payment_method", "amount", "payment_status", "paid_at"],
}


def _seed(*parts: object) -> int:
    """Stable seed from any values (Python's hash() changes between runs)."""
    digest = hashlib.sha256("|".join(map(str, parts)).encode()).hexdigest()
    return int(digest[:16], 16)


def build_products() -> list[dict]:
    return [
        {"product_id": f"P{i:03d}", "product_name": name, "category": category, "unit_price": price}
        for i, (category, name, price) in enumerate(CATALOG, start=1)
    ]


def build_customer(number: int, as_of: date) -> dict:
    """A customer's details as of a date.

    About 10% of customers relocate on a fixed date during 2026, which gives
    the Type 2 slowly changing dimension real history to track.
    """
    rng = random.Random(_seed("customer", number))
    first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
    region = rng.choices(list(REGIONS), weights=REGION_WEIGHTS)[0]
    city = rng.choice(REGIONS[region])
    phone = rng.choice(PHONE_PREFIXES) + f"{rng.randint(0, 9_999_999):07d}"
    email = f"{first}.{last}{number}@{rng.choice(EMAIL_DOMAINS)}".lower()
    signup = date(2024, 1, 1) + timedelta(days=rng.randint(0, 700))

    if rng.random() < 0.10:
        move_date = date(2026, 1, 1) + timedelta(days=rng.randint(0, 364))
        if as_of >= move_date:
            region = rng.choice([r for r in REGIONS if r != region])
            city = rng.choice(REGIONS[region])

    return {
        "customer_id": f"C{number:05d}",
        "full_name": f"{first} {last}",
        "email": email,
        "phone": phone,
        "region": region,
        "city": city,
        "signup_date": signup.isoformat(),
    }


def generate_day(run_date: date) -> dict[str, list[dict]]:
    """All source records for one business day."""
    rng = random.Random(_seed("orders", run_date.isoformat()))
    products = build_products()

    n_orders = rng.randint(80, 160)
    if run_date.weekday() >= 5:  # weekends are busier
        n_orders = int(n_orders * 1.3)

    orders, items, payments = [], [], []
    customer_numbers: set[int] = set()

    for i in range(1, n_orders + 1):
        order_id = f"ORD-{run_date:%Y%m%d}-{i:04d}"
        customer_number = rng.randint(1, CUSTOMER_POOL_SIZE)
        customer_numbers.add(customer_number)
        order_ts = datetime.combine(run_date, time()) + timedelta(seconds=rng.randint(6 * 3600, 23 * 3600))
        status = rng.choices(ORDER_STATUSES, weights=STATUS_WEIGHTS)[0]
        delivery_fee = Decimal(rng.choice(["0", "15", "20", "25", "35"]))

        total = delivery_fee
        n_lines = rng.choices([1, 2, 3, 4], weights=[55, 25, 12, 8])[0]
        for line in range(1, n_lines + 1):
            product = rng.choice(products)
            quantity = rng.choices([1, 2, 3], weights=[75, 18, 7])[0]
            items.append({
                "order_id": order_id,
                "line_number": line,
                "product_id": product["product_id"],
                "quantity": quantity,
                "unit_price": product["unit_price"],
            })
            total += quantity * Decimal(product["unit_price"])

        orders.append({
            "order_id": order_id,
            "customer_id": f"C{customer_number:05d}",
            "order_ts": order_ts.isoformat(sep=" "),
            "status": status,
            "delivery_fee": f"{delivery_fee:.2f}",
        })
        payments.append({
            "payment_id": f"PAY-{run_date:%Y%m%d}-{i:04d}",
            "order_id": order_id,
            "payment_method": rng.choices(PAYMENT_METHODS, weights=PAYMENT_WEIGHTS)[0],
            "amount": f"{total:.2f}",
            "payment_status": PAYMENT_STATUS_FOR_ORDER[status],
            "paid_at": (order_ts + timedelta(minutes=rng.randint(0, 45))).isoformat(sep=" "),
        })

    customers = [build_customer(n, run_date) for n in sorted(customer_numbers)]
    data = {
        "customers": customers,
        "products": products,
        "orders": orders,
        "order_items": items,
        "payments": payments,
    }
    _inject_dirty_records(rng, data)
    return data


def _inject_dirty_records(rng: random.Random, data: dict[str, list[dict]]) -> None:
    """Corrupt a small, reproducible share of records, as real sources do."""
    orders, items, payments, customers = (
        data["orders"], data["order_items"], data["payments"], data["customers"]
    )
    k = max(1, int(len(orders) * DIRTY_RATE))

    for row in rng.sample(items, k):                 # duplicated order lines
        items.append(dict(row))
    for row in rng.sample(orders, k):                # orders with no customer
        row["customer_id"] = ""
    for row in rng.sample(items, k):                 # zero or negative quantity
        row["quantity"] = rng.choice([0, -1])
    for row in rng.sample(items, 2):                 # product not in the catalog
        row["product_id"] = "P999"
    for row in rng.sample(payments, k):              # non-numeric amount
        row["amount"] = "N/A"
    for row in rng.sample(customers, k):             # malformed email (cleansed, not rejected)
        row["email"] = row["email"].replace("@", "_at_")
    customers.append(dict(rng.choice(customers)))    # duplicated customer record


def write_csvs(data: dict[str, list[dict]], out_dir: Path) -> dict[str, str]:
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = {}
    for table, rows in data.items():
        path = out_dir / f"{table}.csv"
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=COLUMNS[table])
            writer.writeheader()
            writer.writerows(rows)
        paths[table] = str(path)
    return paths


def extract(run_date: date, raw_dir: Path) -> dict[str, str]:
    """Extract step: land one day of source data as CSV files."""
    return write_csvs(generate_day(run_date), raw_dir / run_date.isoformat())


def purge_raw(raw_dir: Path, older_than: date) -> list[str]:
    """Delete landed batch folders (named yyyy-mm-dd) dated before `older_than`."""
    purged = []
    for folder in sorted(raw_dir.glob("????-??-??")):
        try:
            batch = date.fromisoformat(folder.name)
        except ValueError:
            continue
        if folder.is_dir() and batch < older_than:
            shutil.rmtree(folder)
            purged.append(folder.name)
    return purged
