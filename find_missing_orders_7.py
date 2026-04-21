"""
find_missing_orders_7.py
------------------------
Reconciles order IDs between the ERP system (GOERP) and the data warehouse
(dwh_0). Identifies orders present in ERP but missing from dbo.FACT_ORDER_INCOME
and orders in the warehouse that have no matching ERP record.

Outputs:
- Console summary of counts and individual IDs.
- Timestamped Excel workbook with two sheets: Missing_in_WH and Extra_in_WH.
- Reloads dbo.TEMP_MISSING_ORDERS_BACKFILL with the missing order IDs so that
  downstream ETL processes can pick them up for backfilling.
"""

import pandas as pd
import pyodbc
from datetime import datetime, timedelta
from pathlib import Path

# ---------------------------------------------------------------------------
# Connection parameters
# ---------------------------------------------------------------------------
ERP_SERVER = "vh02-vm01,1433"
ERP_DB     = "GOERP"
WH_SERVER  = "vh02-vm01,1435"
WH_DB      = "dwh_0"

ERP_USER = "SO"
ERP_PASS = "opossum"
WH_USER  = "dwh_0"
WH_PASS  = "godesys"

OUTPUT_DIR = Path(r"D:\reconsilation")


def connect(server, db, user, pwd):
    """Return a pyodbc connection to the given SQL Server instance.

    Args:
        server: Host and port string, e.g. ``"vh02-vm01,1433"``.
        db:     Database name.
        user:   SQL Server login username.
        pwd:    SQL Server login password.

    Returns:
        An open :class:`pyodbc.Connection`.
    """
    return pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};DATABASE={db};"
        f"UID={user};PWD={pwd};"
    )


# ---------------------------------------------------------------------------
# Fetch order IDs from both sources
# ---------------------------------------------------------------------------

erp_conn = connect(ERP_SERVER, ERP_DB, ERP_USER, ERP_PASS)
wh_conn  = connect(WH_SERVER,  WH_DB,  WH_USER,  WH_PASS)

# Pull every distinct order from ERP, joined to get the order-type description.
# Only orders with a non-null LFDANGAUFGUTNR recorded on or after 2021-01-10
# are considered.
erp_df = pd.read_sql(
    """
    SELECT DISTINCT
        AUFTRAG.LFDANGAUFGUTNR  AS ORDER_ID,
        AUFTRAG.BESTDATUM,
        AUFTRAG.ERFASSDATUM,
        AAGAUFART.AUFARTBEZ,
        AUFTRAG.GUTSCHRIFT,
        AUFTRAG.AUFNR
    FROM AUFTRAG
    JOIN ANGAUFGUT  ON ANGAUFGUT.LFDNR  = AUFTRAG.LFDANGAUFGUTNR
    JOIN AAGAUFART  ON AAGAUFART.LFDNR  = ANGAUFGUT.AAGAUFART
    WHERE AUFTRAG.LFDANGAUFGUTNR IS NOT NULL
      AND AUFTRAG.BESTDATUM >= '2021-01-10'
    """,
    erp_conn
)
erp_df["ORDER_ID"] = erp_df["ORDER_ID"].astype(int).astype(str)
erp_ids = set(erp_df["ORDER_ID"])

# Pull the distinct order IDs that already exist in the warehouse fact table.
wh_ids = set(
    str(int(v)) for v in
    pd.read_sql("SELECT DISTINCT ORDER_ID FROM dbo.FACT_ORDER_INCOME WHERE ORDER_ID IS NOT NULL", wh_conn)["ORDER_ID"]
)

erp_conn.close()
wh_conn.close()

# ---------------------------------------------------------------------------
# Set-difference analysis
# ---------------------------------------------------------------------------

missing_ids    = sorted(erp_ids - wh_ids, key=lambda x: int(x))   # in ERP, not in WH
extra_in_wh    = sorted(wh_ids - erp_ids, key=lambda x: int(x))    # in WH, not in ERP

# Build a detail dataframe for orders that are missing in the warehouse,
# deduplicated and sorted by ORDER_ID for deterministic output.
missing_df = (
    erp_df[erp_df["ORDER_ID"].isin(missing_ids)]
    .drop_duplicates(subset="ORDER_ID")
    .sort_values("ORDER_ID", key=lambda s: s.astype(int))
    .reset_index(drop=True)
)

# ---------------------------------------------------------------------------
# Date filtering: keep orders created from 2021-01-10 up to one full ISO week
# before the current execution date so that incomplete current-week data is
# excluded from the backfill scope.
# ---------------------------------------------------------------------------
missing_df["ERFASSDATUM"] = pd.to_datetime(missing_df["ERFASSDATUM"])

_today = datetime.now().date()
_current_week_start = pd.Timestamp(_today - timedelta(days=_today.weekday()))
_one_week_before    = _current_week_start - timedelta(days=7)
_range_start        = pd.Timestamp("2021-01-10")
missing_df = missing_df[
    (missing_df["ERFASSDATUM"] >= _range_start) &
    (missing_df["ERFASSDATUM"] <  _one_week_before)
]

# ---------------------------------------------------------------------------
# Derived calendar columns
# ---------------------------------------------------------------------------

# ISO 8601 week number (week starts Monday, week 1 contains the first Thursday).
missing_df["KW"] = missing_df["ERFASSDATUM"].dt.isocalendar().week.astype("Int64")


def _week_label(dt):
    """Return a human-readable week-period string for the given date.

    Args:
        dt: A :class:`pandas.Timestamp` or ``NaT``.

    Returns:
        A string of the form ``"05 April until 11 Apr"``, or ``None`` when
        *dt* is ``NaT``.
    """
    if pd.isna(dt):
        return None
    monday = dt - pd.Timedelta(days=dt.weekday())
    sunday = monday + pd.Timedelta(days=6)
    return f"{monday.strftime('%d %B')} until {sunday.strftime('%d %b')}"


missing_df["Week_Period"] = missing_df["ERFASSDATUM"].apply(_week_label)

# Year and month of the order creation date, stored as nullable integers.
missing_df["Creation_Year"]  = missing_df["ERFASSDATUM"].dt.year.astype("Int64")
missing_df["Creation_Month"] = missing_df["ERFASSDATUM"].dt.month.astype("Int64")

# ---------------------------------------------------------------------------
# Console summary
# ---------------------------------------------------------------------------

print(f"ERP orders        : {len(erp_ids)}")
print(f"WH orders         : {len(wh_ids)}")
print(f"Missing in WH     : {len(missing_ids)}")
print(f"Extra in WH (orphan): {len(extra_in_wh)}")

if missing_ids:
    print("\n--- IN ERP BUT MISSING IN dbo.FACT_ORDER_INCOME ---")
    for oid in missing_ids:
        print(f"  {oid}")

if extra_in_wh:
    print("\n--- IN dbo.FACT_ORDER_INCOME BUT NOT IN ERP ---")
    for oid in extra_in_wh:
        print(f"  {oid}")

# ---------------------------------------------------------------------------
# Excel export
# ---------------------------------------------------------------------------

timestamp   = datetime.now().strftime("%Y%m%d_%H%M%S")
output_file = OUTPUT_DIR / f"missing_order_ids_{timestamp}.xlsx"

# Sheet 1 – full detail for orders in ERP but absent from the warehouse.
# Sheet 2 – plain ID list for warehouse rows with no ERP counterpart.
with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
    missing_df.to_excel(writer, sheet_name="Missing_in_WH", index=False)
    pd.DataFrame({"ORDER_ID": extra_in_wh}).to_excel(writer, sheet_name="Extra_in_WH", index=False)

print(f"\nExported to: {output_file}")

# ---------------------------------------------------------------------------
# Reload staging table for downstream ETL backfill
# ---------------------------------------------------------------------------

# Truncate the staging table and insert the current set of missing IDs so that
# the scheduled ETL job can use it without needing to recompute the delta.
wh_conn = connect(WH_SERVER, WH_DB, WH_USER, WH_PASS)
cursor = wh_conn.cursor()
cursor.execute("TRUNCATE TABLE dbo.TEMP_MISSING_ORDERS_BACKFILL")
if not missing_df.empty:
    ids = [(int(oid),) for oid in missing_df["ORDER_ID"]]
    cursor.executemany(
        "INSERT INTO dbo.TEMP_MISSING_ORDERS_BACKFILL (ID) VALUES (?)", ids
    )
wh_conn.commit()
cursor.close()
wh_conn.close()
print(f"Loaded {len(missing_df)} rows into dbo.TEMP_MISSING_ORDERS_BACKFILL")
