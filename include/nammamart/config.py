"""
include/nammamart/config.py
---------------------------
Paths, owner name and business rules shared by the NammaMart ETL and ELT DAGs.

Paths are derived from this file's location, so the same code runs inside the
Astro containers (/usr/local/airflow/include/...) and on a laptop for testing.
"""

import os
import time
from pathlib import Path

LEARNER = "ashwani"

INCLUDE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = Path(os.getenv("NAMMAMART_DATA_DIR", INCLUDE_DIR / "data" / "nammamart"))
WAREHOUSE_DB = Path(os.getenv("NAMMAMART_WAREHOUSE", INCLUDE_DIR / f"warehouse_{LEARNER}.duckdb"))
QUARANTINE_DIR = Path(os.getenv("NAMMAMART_QUARANTINE_DIR", INCLUDE_DIR / "quarantine" / LEARNER))
WORK_DIR = Path(os.getenv("NAMMAMART_WORK_DIR", INCLUDE_DIR / "tmp" / LEARNER))
SQL_DIR = INCLUDE_DIR / "sql" / "elt"

# The five daily exports, and the glob for next-day incremental order files.
SOURCE_FILES = {
    "orders": "orders.csv",
    "payments": "payments.csv",
    "customers": "customers.csv",
    "products": "products.csv",
    "stores": "stores.csv",
}
INCREMENT_GLOB = "orders_*_increment.csv"

# Expected columns per file. Extract fails loudly if any are missing (schema check).
EXPECTED_COLUMNS = {
    "orders": ["order_id", "order_ts", "customer_id", "store_id", "product_id", "quantity",
               "unit_price", "discount_pct", "order_amount", "payment_mode", "order_status",
               "channel", "delivery_minutes"],
    "payments": ["payment_id", "order_id", "payment_ts", "payment_mode", "amount_paid",
                 "payment_status", "gateway_ref"],
    "customers": ["customer_id", "full_name", "email", "phone", "city", "area", "pincode",
                  "signup_date", "loyalty_tier", "is_active"],
    "products": ["product_id", "product_name", "category", "sub_category", "brand", "mrp",
                 "cost_price", "unit", "is_perishable", "launch_date"],
    "stores": ["store_id", "store_name", "zone", "area", "pincode", "open_date",
               "manager_name", "capacity_orders_per_day"],
}

# Business rules agreed with the client (Section 3 of the brief).
VALID_ORDER_STATUS = ["DELIVERED", "CANCELLED", "RETURNED"]
VALID_PAYMENT_MODE = ["UPI", "Card", "Cash on Delivery", "Wallet"]
VALID_PAYMENT_STATUS = ["SUCCESS", "FAILED", "REFUNDED"]
VALID_LOYALTY_TIER = ["Bronze", "Silver", "Gold", "Platinum"]
VALID_ZONE = ["North", "South", "East", "West"]

# Known spellings of payment modes seen in the exports, mapped to the allowed value.
PAYMENT_MODE_ALIASES = {
    "upi": "UPI",
    "card": "Card",
    "cash on delivery": "Cash on Delivery",
    "cod": "Cash on Delivery",
    "cash": "Cash on Delivery",
    "cashh": "Cash on Delivery",
    "wallet": "Wallet",
}

MAX_DELIVERY_MINUTES = 120      # Accuracy: anything above is a data error (client to confirm)
SLA_MINUTES = 15                # "within 15 minutes" KPI
NORMAL_DELIVERY_RANGE = (8, 35) # Outside this range (but under 120) is a Warning
PRICE_DRIFT_RATIO = 1.5         # unit_price more than 1.5x (or under 1/1.5x) MRP is a Warning
VOLUME_DROP_PCT = 90            # Warn if today's order rows fall >90% vs the last good run
FRESHNESS_HOURS = 24            # Warn if the newest order is older than this at run time


def run_slug(run_id: str) -> str:
    """Airflow run ids contain ':' and '+', which are awkward in file names."""
    return "".join(ch if ch.isalnum() or ch in "-_." else "_" for ch in run_id)


def connect(read_only: bool = False, attempts: int = 90, wait_seconds: float = 2.0):
    """
    Open the DuckDB warehouse, retrying while another process holds the write lock.

    DuckDB allows one writer at a time. The ETL and ELT DAGs both run at 9:00 AM and
    share this file, so a task may briefly find it locked. Retrying is simpler than
    an Airflow pool and needs no extra setup.
    """
    import duckdb

    WAREHOUSE_DB.parent.mkdir(parents=True, exist_ok=True)
    last_error = None
    for _ in range(attempts):
        try:
            return duckdb.connect(str(WAREHOUSE_DB), read_only=read_only)
        except duckdb.IOException as exc:  # lock held by another process
            last_error = exc
            time.sleep(wait_seconds)
    raise last_error
