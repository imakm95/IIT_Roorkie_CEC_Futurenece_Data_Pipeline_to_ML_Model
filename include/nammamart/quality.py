"""
include/nammamart/quality.py
----------------------------
Reading, standardising, validating and masking NammaMart data in pandas (ETL side).

Every check returns a boolean Series per row, keyed by the check code from
dq_catalog.CHECKS. `split_by_quality` turns those flags into:
  failure_reason  -> semicolon-separated Critical problems (row is quarantined)
  warning_reason  -> semicolon-separated Warning problems (row loads)
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from nammamart import config
from nammamart.dq_catalog import CHECKS, checks_for, is_critical, reason

TS_FORMAT = "%Y-%m-%d %H:%M:%S"
DATE_FORMAT = "%Y-%m-%d"
EMAIL_RE = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
PHONE_RE = r"^[6-9][0-9]{9}$"
PINCODE_RE = r"^560[0-9]{3}$"


# --------------------------------------------------------------------------- read

def read_source(path: Path, file_key: str) -> pd.DataFrame:
    """Read a CSV exactly as text, check its columns, and add lineage columns."""
    if not Path(path).exists():
        raise FileNotFoundError(f"Source file missing: {path}")
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    missing = [c for c in config.EXPECTED_COLUMNS[file_key] if c not in df.columns]
    if missing:
        # Schema check: fail loudly instead of loading wrong data silently.
        raise ValueError(
            f"Schema change in {Path(path).name}: missing column(s) {missing}. "
            f"Found columns {list(df.columns)}"
        )
    df["_source_file"] = Path(path).name
    df["_row_num"] = range(1, len(df) + 1)  # 1-based line number after the header
    return df


def order_files(data_dir: Path = config.DATA_DIR) -> list[Path]:
    """orders.csv first, then every increment file in date order (file name sorts by date)."""
    return [data_dir / config.SOURCE_FILES["orders"]] + sorted(data_dir.glob(config.INCREMENT_GLOB))


# --------------------------------------------------------------------- standardise

def _clean_text(s: pd.Series) -> pd.Series:
    s = s.astype("string").str.strip()
    return s.mask(s == "")


def standardise(df: pd.DataFrame, file_key: str) -> pd.DataFrame:
    """Trim every value, turn blanks into nulls and fix known spellings/casing."""
    out = df.copy()
    for col in config.EXPECTED_COLUMNS[file_key]:
        out[col + "_raw"] = out[col]
        out[col] = _clean_text(out[col])
    if "payment_mode" in out:
        lower = out["payment_mode"].str.lower()
        out["payment_mode"] = lower.map(config.PAYMENT_MODE_ALIASES).fillna(out["payment_mode"])
    if "order_status" in out:
        out["order_status"] = out["order_status"].str.upper()
    if "loyalty_tier" in out:
        out["loyalty_tier"] = out["loyalty_tier"].str.title()
    if "zone" in out:
        out["zone"] = out["zone"].str.title()
    return out


def _num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def _ts(s: pd.Series, fmt: str = TS_FORMAT) -> pd.Series:
    return pd.to_datetime(s, format=fmt, errors="coerce")


def _changed(df: pd.DataFrame, col: str) -> pd.Series:
    """True where standardising changed a value (casing / spelling / spaces)."""
    return df[col].notna() & (df[col + "_raw"].astype("string") != df[col])


def _dup_rank(df: pd.DataFrame, key: str, extra_order: list[str] | None = None) -> pd.Series:
    """1 for the row we keep per key, 2+ for extra copies (null keys get rank 1)."""
    order = (extra_order or []) + ["_row_num"]
    ranked = df.sort_values(order).groupby([df["_source_file"], df[key]], dropna=False).cumcount() + 1
    ranked = ranked.reindex(df.index)
    return ranked.where(df[key].notna(), 1)


# ------------------------------------------------------------------------ checks

def check_customers(df: pd.DataFrame, run_ts: pd.Timestamp) -> dict[str, pd.Series]:
    df["_dup_rank"] = _dup_rank(df, "customer_id")
    signup = _ts(df["signup_date"], DATE_FORMAT)
    return {
        "cus_customer_id_not_null": df["customer_id"].isna(),
        "cus_customer_id_unique": df["_dup_rank"] > 1,
        "cus_email_format": ~df["email"].fillna("").str.match(EMAIL_RE),
        "cus_phone_10_digits": ~df["phone"].fillna("").str.match(PHONE_RE),
        "cus_pincode_6_digits": ~df["pincode"].fillna("").str.match(PINCODE_RE),
        "cus_city_present": df["city"].isna(),
        "cus_tier_casing": _changed(df, "loyalty_tier"),
        "cus_signup_not_future": signup > run_ts.normalize(),
    }


def check_products(df: pd.DataFrame, run_ts: pd.Timestamp) -> dict[str, pd.Series]:
    # Keep the current row for a duplicated product_id; rows named "(old)" lose.
    df["_is_old"] = df["product_name"].fillna("").str.lower().str.endswith("(old)").astype(int)
    df["_dup_rank"] = _dup_rank(df, "product_id", ["_is_old"])
    mrp, cost = _num(df["mrp"]), _num(df["cost_price"])
    return {
        "prd_product_id_not_null": df["product_id"].isna(),
        "prd_product_id_unique": df["_dup_rank"] > 1,
        "prd_category_present": df["category"].isna(),
        "prd_mrp_positive": ~(mrp > 0),
        "prd_cost_le_mrp": cost > mrp,
    }


def check_stores(df: pd.DataFrame, run_ts: pd.Timestamp) -> dict[str, pd.Series]:
    df["_dup_rank"] = _dup_rank(df, "store_id")
    return {
        "sto_store_id_not_null": df["store_id"].isna(),
        "sto_store_id_unique": df["_dup_rank"] > 1,
        "sto_zone_valid": ~df["zone"].isin(config.VALID_ZONE),
        "sto_capacity_positive": ~(_num(df["capacity_orders_per_day"]) > 0),
    }


def check_orders(df, run_ts, customers, products, stores, received_rows, previous_rows=None):
    """Row-level order checks, plus dataset-level checks returned separately."""
    qty, price, disc = _num(df["quantity"]), _num(df["unit_price"]), _num(df["discount_pct"])
    amount, minutes = _num(df["order_amount"]), _num(df["delivery_minutes"])
    ts = _ts(df["order_ts"])
    expected = qty * price * (1 - disc / 100)

    mrp = df["product_id"].map(
        products.loc[products["_dup_rank"] == 1].set_index("product_id")["mrp"].pipe(_num)
    )
    ratio = price / mrp.where(mrp > 0)
    lo, hi = config.NORMAL_DELIVERY_RANGE

    # Store capacity: orders per store per day above capacity_orders_per_day (bonus check).
    cap = df["store_id"].map(stores.loc[stores["_dup_rank"] == 1].set_index("store_id")["capacity_orders_per_day"].pipe(_num))
    per_day = df.groupby([df["store_id"], ts.dt.date], dropna=False)["order_id"].transform("count")

    flags = {
        "ord_order_id_not_null": df["order_id"].isna(),
        "ord_customer_id_not_null": df["customer_id"].isna(),
        "ord_delivered_has_minutes": (df["order_status"] == "DELIVERED") & minutes.isna(),
        "ord_order_id_unique": df["_dup_rank"] > 1,
        "ord_ts_parses": ts.isna(),
        "ord_status_valid": ~df["order_status"].isin(config.VALID_ORDER_STATUS),
        "ord_payment_mode_valid": ~df["payment_mode"].isin(config.VALID_PAYMENT_MODE),
        "ord_discount_range": ~disc.between(0, 100),
        "ord_quantity_positive": ~(qty > 0),
        "ord_amount_formula": ~((amount - expected).abs() <= 0.0100001),
        "ord_delivery_minutes_max": minutes.notna() & ((minutes > config.MAX_DELIVERY_MINUTES) | (minutes < 0)),
        "ord_customer_fk": df["customer_id"].notna() & ~df["customer_id"].isin(customers["customer_id"].dropna()),
        "ord_store_fk": ~df["store_id"].isin(stores["store_id"].dropna()),
        "ord_product_fk": ~df["product_id"].isin(products["product_id"].dropna()),
        "ord_ts_not_future": ts > run_ts,
        "ord_status_casing": _changed(df, "order_status") & df["order_status"].isin(config.VALID_ORDER_STATUS),
        "ord_payment_mode_alias": _changed(df, "payment_mode") & df["payment_mode"].isin(config.VALID_PAYMENT_MODE),
        "ord_delivery_minutes_unusual": minutes.between(0, config.MAX_DELIVERY_MINUTES) & ~minutes.between(lo, hi),
        "ord_price_vs_mrp": (ratio > config.PRICE_DRIFT_RATIO) | (ratio < 1 / config.PRICE_DRIFT_RATIO),
        "ord_store_capacity": (cap > 0) & (per_day > cap),
    }

    newest = ts[ts <= run_ts].max()
    stale = pd.isna(newest) or newest < run_ts - pd.Timedelta(hours=config.FRESHNESS_HOURS)
    dropped = bool(previous_rows) and received_rows < previous_rows * (1 - config.VOLUME_DROP_PCT / 100)
    dataset = {
        "ord_freshness": (1, int(stale), f"newest valid order_ts = {newest}"),
        "ord_volume_drop": (received_rows, int(dropped), f"rows now {received_rows}, last good run {previous_rows}"),
    }
    return flags, dataset


def check_payments(df, run_ts, orders_all, orders_kept):
    amount, ts = _num(df["amount_paid"]), _ts(df["payment_ts"])
    first_copy = orders_kept.loc[orders_kept["_dup_rank"] == 1].set_index("order_id")
    order_amount = df["order_id"].map(_num(first_copy["order_amount"]))
    return {
        "pay_payment_id_not_null": df["payment_id"].isna(),
        "pay_payment_id_unique": df["_dup_rank"] > 1,
        "pay_ts_parses": ts.isna(),
        "pay_mode_valid": ~df["payment_mode"].isin(config.VALID_PAYMENT_MODE),
        "pay_status_valid": ~df["payment_status"].isin(config.VALID_PAYMENT_STATUS),
        "pay_amount_non_negative": ~(amount >= 0),
        "pay_order_fk": ~df["order_id"].isin(orders_all["order_id"].dropna()),
        "pay_ts_not_future": ts > run_ts,
        "pay_mode_alias": _changed(df, "payment_mode") & df["payment_mode"].isin(config.VALID_PAYMENT_MODE),
        "pay_gateway_ref_missing": (df["payment_mode"] != "Cash on Delivery") & df["gateway_ref"].isna(),
        "pay_amount_matches_order": (df["payment_status"] == "SUCCESS") & order_amount.notna()
                                    & ((amount - order_amount).abs() > 0.01),
    }


# ------------------------------------------------------------------- apply flags

def apply_flags(df: pd.DataFrame, flags: dict[str, pd.Series]) -> pd.DataFrame:
    """Add failure_reason / warning_reason / dq_status columns from the check flags."""
    out = df.copy()
    crit, warn = [], []
    for code in CHECKS:  # catalog order, so reason text matches the ELT pipeline
        if code not in flags:
            continue
        mask = flags[code].reindex(out.index).fillna(False).astype(bool)
        text = pd.Series("", index=out.index).mask(mask, reason(code))
        (crit if is_critical(code) else warn).append(text)

    def join(parts):
        if not parts:
            return pd.Series(pd.NA, index=out.index, dtype="string")
        joined = pd.concat(parts, axis=1).apply(lambda r: "; ".join(x for x in r if x), axis=1)
        return joined.mask(joined == "").astype("string")

    out["failure_reason"] = join(crit)
    out["warning_reason"] = join(warn)
    out["dq_status"] = "PASS"
    out.loc[out["warning_reason"].notna(), "dq_status"] = "WARN"
    out.loc[out["failure_reason"].notna(), "dq_status"] = "FAIL"
    return out


def dq_rows(file_key: str, n_rows: int, flags: dict[str, pd.Series], dataset=None) -> list[dict]:
    """One dq_results row per check for this file."""
    rows = []
    for code, spec in checks_for(file_key).items():
        _, name, dim, sev, _ = spec
        if code in flags:
            checked, failed = n_rows, int(flags[code].fillna(False).sum())
        elif dataset and code in dataset:
            checked, failed, _note = dataset[code]
        else:
            continue
        rows.append({
            "check_code": code, "check_name": f"{file_key}: {name}", "dimension": dim,
            "severity": sev, "rows_checked": int(checked), "rows_failed": int(failed),
            "status": "PASS" if failed == 0 else ("FAIL" if sev == "Critical" else "WARN"),
        })
    return rows


# ------------------------------------------------------------------------ masking

def mask_email(email):
    if email is None or pd.isna(email) or not pd.Series([email]).str.match(EMAIL_RE).iloc[0]:
        return None
    local, domain = str(email).split("@", 1)
    return f"{local[:2]}***@{domain}"


def mask_phone(phone):
    if phone is None or pd.isna(phone):
        return None
    p = str(phone)
    if len(p) <= 4:
        return "*" * len(p)
    return p[:2] + "*" * (len(p) - 4) + p[-2:]


def mask_name(name):
    """'Priya Hegde' -> 'Priya H.' so reports stay readable without the full name."""
    if name is None or pd.isna(name):
        return None
    parts = str(name).split()
    return parts[0] if len(parts) == 1 else f"{parts[0]} {parts[-1][0]}."


ALL_CODES = list(CHECKS)
