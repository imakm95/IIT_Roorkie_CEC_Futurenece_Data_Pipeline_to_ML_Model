"""
include/nammamart/dq_catalog.py
-------------------------------
The single list of data quality checks used by BOTH pipelines.

ETL implements each check in pandas (include/nammamart/etl_checks.py).
ELT implements the same check in SQL (include/sql/elt/staging/*.sql) and tags the
failing rows with the same check code. Because both pipelines share codes, their
`dq_results` rows can be compared one to one.

Severity
  Critical: the row is quarantined (ETL) or marked dq_status = 'FAIL' (ELT) and never
            reaches a finance/reporting table.
  Warning:  the row loads, and the issue is counted in dq_results.

Design rule (agreed assumption, see SUBMISSION.md, client question 3):
  For master files (customers, products, stores) only key problems are Critical.
  Attribute problems (bad email, missing zone, cost above MRP) are Warnings, because
  quarantining a master row would also reject every valid, paid order that points to it.
"""

# code: (file, check name shown in dq_results, dimension, severity, reason text)
CHECKS = {
    # ---------------- orders (incl. increment files) ----------------
    "ord_order_id_not_null": ("orders", "order_id is present", "Completeness", "Critical", "order_id is missing"),
    "ord_customer_id_not_null": ("orders", "customer_id is present", "Completeness", "Critical", "customer_id is missing"),
    "ord_delivered_has_minutes": ("orders", "DELIVERED order has delivery_minutes", "Completeness", "Critical", "DELIVERED order has no delivery_minutes"),
    "ord_order_id_unique": ("orders", "order_id is unique within a file", "Uniqueness", "Critical", "duplicate order_id (extra copy)"),
    "ord_ts_parses": ("orders", "order_ts parses as YYYY-MM-DD HH:MM:SS", "Validity", "Critical", "order_ts is not a valid timestamp"),
    "ord_status_valid": ("orders", "order_status in allowed list", "Validity", "Critical", "order_status not in DELIVERED/CANCELLED/RETURNED"),
    "ord_payment_mode_valid": ("orders", "payment_mode in allowed list", "Validity", "Critical", "payment_mode not in allowed list"),
    "ord_discount_range": ("orders", "discount_pct between 0 and 100", "Validity", "Critical", "discount_pct outside 0-100"),
    "ord_quantity_positive": ("orders", "quantity > 0", "Accuracy", "Critical", "quantity is zero or negative"),
    "ord_amount_formula": ("orders", "order_amount = qty x price x (1 - discount)", "Accuracy", "Critical", "order_amount does not match qty x unit_price x (1 - discount)"),
    "ord_delivery_minutes_max": ("orders", "delivery_minutes between 0 and 120", "Accuracy", "Critical", "delivery_minutes above 120 or negative"),
    "ord_customer_fk": ("orders", "customer_id exists in customers", "Consistency", "Critical", "customer_id not in customer master"),
    "ord_store_fk": ("orders", "store_id exists in stores", "Consistency", "Critical", "store_id not in store master"),
    "ord_product_fk": ("orders", "product_id exists in products", "Consistency", "Critical", "product_id not in product master"),
    "ord_ts_not_future": ("orders", "order_ts not after run time", "Timeliness", "Critical", "order_ts is in the future"),
    "ord_status_casing": ("orders", "order_status uses standard casing", "Consistency", "Warning", "order_status casing standardised"),
    "ord_payment_mode_alias": ("orders", "payment_mode uses standard spelling", "Consistency", "Warning", "payment_mode spelling standardised"),
    "ord_delivery_minutes_unusual": ("orders", "delivery_minutes within normal 8-35", "Accuracy", "Warning", "delivery_minutes outside normal 8-35 range"),
    "ord_price_vs_mrp": ("orders", "unit_price within 1.5x of product MRP", "Accuracy", "Warning", "unit_price far from product MRP (possible price drift)"),
    "ord_freshness": ("orders", "latest order_ts within 24h of run", "Timeliness", "Warning", "newest order is older than 24 hours"),
    "ord_volume_drop": ("orders", "order rows not down >90% vs last run", "Completeness", "Warning", "order volume dropped more than 90%"),
    "ord_store_capacity": ("orders", "daily orders within store capacity", "Accuracy", "Warning", "store took more orders in a day than its capacity"),
    # ---------------- payments ----------------
    "pay_payment_id_not_null": ("payments", "payment_id is present", "Completeness", "Critical", "payment_id is missing"),
    "pay_payment_id_unique": ("payments", "payment_id is unique", "Uniqueness", "Critical", "duplicate payment_id (extra copy)"),
    "pay_ts_parses": ("payments", "payment_ts parses as a timestamp", "Validity", "Critical", "payment_ts is not a valid timestamp"),
    "pay_mode_valid": ("payments", "payment_mode in allowed list", "Validity", "Critical", "payment_mode not in allowed list"),
    "pay_status_valid": ("payments", "payment_status in SUCCESS/FAILED/REFUNDED", "Validity", "Critical", "payment_status not in allowed list"),
    "pay_amount_non_negative": ("payments", "amount_paid >= 0", "Accuracy", "Critical", "amount_paid is negative"),
    "pay_order_fk": ("payments", "order_id exists in orders", "Consistency", "Critical", "order_id not found in orders"),
    "pay_ts_not_future": ("payments", "payment_ts not after run time", "Timeliness", "Critical", "payment_ts is in the future"),
    "pay_mode_alias": ("payments", "payment_mode uses standard spelling", "Consistency", "Warning", "payment_mode spelling standardised"),
    "pay_gateway_ref_missing": ("payments", "non-cash payment has gateway_ref", "Completeness", "Warning", "gateway_ref missing for a non-cash payment"),
    "pay_amount_matches_order": ("payments", "SUCCESS amount equals order_amount", "Consistency", "Warning", "SUCCESS amount differs from order_amount"),
    # ---------------- customers ----------------
    "cus_customer_id_not_null": ("customers", "customer_id is present", "Completeness", "Critical", "customer_id is missing"),
    "cus_customer_id_unique": ("customers", "one row per customer_id", "Uniqueness", "Critical", "duplicate customer_id (extra copy)"),
    "cus_email_format": ("customers", "email has a valid format", "Validity", "Warning", "email format invalid"),
    "cus_phone_10_digits": ("customers", "phone is 10 digits starting 6-9", "Validity", "Warning", "phone is not a 10-digit mobile number"),
    "cus_pincode_6_digits": ("customers", "pincode is a 6-digit Bengaluru pincode", "Validity", "Warning", "pincode invalid"),
    "cus_city_present": ("customers", "city is present", "Completeness", "Warning", "city is missing"),
    "cus_tier_casing": ("customers", "loyalty_tier uses one casing", "Consistency", "Warning", "loyalty_tier casing standardised"),
    "cus_signup_not_future": ("customers", "signup_date not in the future", "Timeliness", "Warning", "signup_date is in the future"),
    # ---------------- products ----------------
    "prd_product_id_not_null": ("products", "product_id is present", "Completeness", "Critical", "product_id is missing"),
    "prd_product_id_unique": ("products", "one row per product_id", "Uniqueness", "Critical", "duplicate product_id (older copy)"),
    "prd_category_present": ("products", "category is present", "Completeness", "Warning", "category is missing"),
    "prd_mrp_positive": ("products", "mrp > 0", "Accuracy", "Warning", "mrp is zero or negative"),
    "prd_cost_le_mrp": ("products", "cost_price <= mrp", "Accuracy", "Warning", "cost_price is above mrp"),
    # ---------------- stores ----------------
    "sto_store_id_not_null": ("stores", "store_id is present", "Completeness", "Critical", "store_id is missing"),
    "sto_store_id_unique": ("stores", "one row per store_id", "Uniqueness", "Critical", "duplicate store_id (extra copy)"),
    "sto_zone_valid": ("stores", "zone in North/South/East/West", "Validity", "Warning", "zone missing or invalid"),
    "sto_capacity_positive": ("stores", "capacity_orders_per_day > 0", "Accuracy", "Warning", "capacity_orders_per_day is zero"),
}


def checks_for(file_key: str):
    return {code: spec for code, spec in CHECKS.items() if spec[0] == file_key}


def is_critical(code: str) -> bool:
    return CHECKS[code][3] == "Critical"


def reason(code: str) -> str:
    return CHECKS[code][4]


def catalog_rows():
    """Rows for the dq_check_catalog warehouse table (used by the ELT SQL)."""
    return [(code, *spec) for code, spec in CHECKS.items()]
