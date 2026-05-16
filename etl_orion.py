

import json
import ijson
import pandas as pd
import numpy as np
from pathlib import Path
import logging
import time

SALES_PATH    = Path("data/Sales.json")
FORECAST_PATH = Path("data/forecast.json")
OUTPUT_DIR    = Path("outputs")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Helpers ─────────────────────────────────────────────────────────────────

def strip_and_nullify_empty_strings(val):
    """Strip whitespace; return None for empty/null values."""
    if val is None:
        return None
    val = str(val).strip()
    return val if val else None


def parse_order_date_to_timestamp(raw_date_string):
    """Parse M/D/YYYY strings → pd.Timestamp, coerce bad values to NaT."""
    try:
        return pd.to_datetime(raw_date_string, format="%m/%d/%Y", errors="coerce")
    except Exception:
        return pd.NaT



log.info("── STEP 1: Extracting raw data ─────────────────────────────────")

log.info("Streaming Sales.json …")
pipeline_start_time = time.time()
raw_sales_records = []
with open(SALES_PATH, "rb") as f:
    for record in ijson.items(f, "item"):
        raw_sales_records.append(record)
log.info(f"  Loaded {len(raw_sales_records):,} sales records in {time.time()-pipeline_start_time:.1f}s")

# --- Forecast: small file, load directly ---
log.info("Loading forecast.json …")
with open(FORECAST_PATH, "r", encoding="utf-8") as f:
    raw_forecast_records = json.load(f)
log.info(f"  Loaded {len(raw_forecast_records):,} forecast records")


# ═══════════════════════════════════════════════════════════════════════════
# STEP 2 – DATA QUALITY REPORT
# ═══════════════════════════════════════════════════════════════════════════
log.info("── STEP 2: Data Quality Exploration ───────────────────────────")

raw_sales_dataframe_for_quality_check = pd.DataFrame(raw_sales_records)

log.info("  Shape          : %s rows × %s cols", *raw_sales_dataframe_for_quality_check.shape)
log.info("  Duplicate rows : %s", raw_sales_dataframe_for_quality_check.duplicated().sum())

null_percentage_per_column = (raw_sales_dataframe_for_quality_check.isnull().sum() / len(raw_sales_dataframe_for_quality_check) * 100).round(1)
null_percentage_per_column = null_percentage_per_column[null_percentage_per_column > 0]
log.info("  Null percentages:\n%s", null_percentage_per_column.to_string())

# Date format sanity check
sample_order_date_values = raw_sales_dataframe_for_quality_check["OrderDate"].dropna().unique()[:5]
log.info("  Sample OrderDate values: %s", list(sample_order_date_values))

# Negative / zero prices
rows_with_invalid_price = (raw_sales_dataframe_for_quality_check["Net Price"] <= 0).sum()
log.info("  Rows with Net Price ≤ 0: %s", rows_with_invalid_price)

# Negative / zero quantities
rows_with_invalid_quantity = (raw_sales_dataframe_for_quality_check["Quantity"] <= 0).sum()
log.info("  Rows with Quantity ≤ 0: %s", rows_with_invalid_quantity)

del raw_sales_dataframe_for_quality_check   # release memory


# ═══════════════════════════════════════════════════════════════════════════
# STEP 3 – TRANSFORM
# ═══════════════════════════════════════════════════════════════════════════
log.info("── STEP 3: Transforming data ───────────────────────────────────")

# ── 3a. Build dim_product ──────────────────────────────────────────────────
log.info("  Building dim_product …")

unique_products_lookup = {}   # ProductKey → row dict
for sales_record in raw_sales_records:
    current_product_key = sales_record["ProductKey"]
    if current_product_key not in unique_products_lookup:
        unique_products_lookup[current_product_key] = {
            "ProductKey"  : current_product_key,
            "ProductName" : strip_and_nullify_empty_strings(sales_record.get("Product Name")),
            "Brand"       : strip_and_nullify_empty_strings(sales_record.get("Brand")),
            "Color"       : strip_and_nullify_empty_strings(sales_record.get("Color")),
            "Subcategory" : strip_and_nullify_empty_strings(sales_record.get("Subcategory")),
            "Category"    : strip_and_nullify_empty_strings(sales_record.get("Category")),
        }

dim_product = pd.DataFrame(unique_products_lookup.values()).sort_values("ProductKey").reset_index(drop=True)

# Data quality fixes
dim_product["ProductName"] = dim_product["ProductName"].fillna("Unknown Product")
dim_product["Brand"]       = dim_product["Brand"].fillna("Unknown Brand")
dim_product["Category"]    = dim_product["Category"].fillna("Unknown Category")
dim_product["Subcategory"] = dim_product["Subcategory"].fillna("Unknown Subcategory")
dim_product["Color"]       = dim_product["Color"].fillna("Unknown Color")

log.info("    %s unique products", len(dim_product))

# ── 3b. Build dim_customer ─────────────────────────────────────────────────
log.info("  Building dim_customer …")

unique_customers_lookup = {}
for sales_record in raw_sales_records:
    current_customer_key = sales_record["CustomerKey"]
    if current_customer_key not in unique_customers_lookup:
        unique_customers_lookup[current_customer_key] = {
            "CustomerKey"   : current_customer_key,
            "CustomerCode"  : strip_and_nullify_empty_strings(sales_record.get("Customer Code")),
            "CustomerName"  : strip_and_nullify_empty_strings(sales_record.get("Name")),
            "Education"     : strip_and_nullify_empty_strings(sales_record.get("Education")),
            "Occupation"    : strip_and_nullify_empty_strings(sales_record.get("Occupation")),
            "Continent"     : strip_and_nullify_empty_strings(sales_record.get("Continent")),
            "City"          : strip_and_nullify_empty_strings(sales_record.get("City")),
            "State"         : strip_and_nullify_empty_strings(sales_record.get("State")),
            "CountryRegion" : strip_and_nullify_empty_strings(sales_record.get("CountryRegion")),
        }
    else:
        # If previously null, try to fill from later records
        existing_customer_record = unique_customers_lookup[current_customer_key]
        if existing_customer_record["CustomerName"] is None:
            existing_customer_record["CustomerName"] = strip_and_nullify_empty_strings(sales_record.get("Name"))
        if existing_customer_record["Education"] is None:
            existing_customer_record["Education"] = strip_and_nullify_empty_strings(sales_record.get("Education"))
        if existing_customer_record["Occupation"] is None:
            existing_customer_record["Occupation"] = strip_and_nullify_empty_strings(sales_record.get("Occupation"))

dim_customer = pd.DataFrame(unique_customers_lookup.values()).sort_values("CustomerKey").reset_index(drop=True)

# Fill remaining nulls with "Unknown"
for col in ["CustomerName", "Education", "Occupation", "Continent", "City", "State", "CountryRegion"]:
    dim_customer[col] = dim_customer[col].fillna("Unknown")

log.info("    %s unique customers", len(dim_customer))

# ── 3c. Build fact_sales ───────────────────────────────────────────────────
log.info("  Building fact_sales …")

cleaned_sales_rows = []
for row_index, sales_record in enumerate(raw_sales_records):
    raw_order_date_string  = sales_record.get("OrderDate")
    parsed_order_date      = parse_order_date_to_timestamp(raw_order_date_string)

    unit_net_price   = float(sales_record.get("Net Price") or 0)
    units_sold       = int(sales_record.get("Quantity") or 0)

    # Skip records with invalid price or quantity
    if unit_net_price <= 0 or units_sold <= 0:
        continue

    cleaned_sales_rows.append({
        "SaleID"      : row_index + 1,
        "ProductKey"  : sales_record["ProductKey"],
        "CustomerKey" : sales_record["CustomerKey"],
        "OrderDate"   : parsed_order_date,
        "Quantity"    : units_sold,
        "NetPrice"    : round(unit_net_price, 4),
        "SalesAmount" : round(unit_net_price * units_sold, 4),
    })

fact_sales = pd.DataFrame(cleaned_sales_rows)

# Drop rows where date couldn't be parsed
rows_with_unparseable_dates = fact_sales["OrderDate"].isna().sum()
if rows_with_unparseable_dates:
    log.warning("    Dropping %s rows with unparseable dates", rows_with_unparseable_dates)
    fact_sales = fact_sales.dropna(subset=["OrderDate"])

# Remove duplicate transactions (same product/customer/date/qty/price)
total_rows_before_deduplication = len(fact_sales)
fact_sales = fact_sales.drop_duplicates(
    subset=["ProductKey", "CustomerKey", "OrderDate", "Quantity", "NetPrice"]
).reset_index(drop=True)
fact_sales["SaleID"] = range(1, len(fact_sales) + 1)
log.info("    Removed %s duplicate rows", total_rows_before_deduplication - len(fact_sales))

# Add Year / Month columns for easier aggregation
fact_sales["Year"]  = fact_sales["OrderDate"].dt.year
fact_sales["Month"] = fact_sales["OrderDate"].dt.month

log.info("    %s fact_sales rows  (removed %s invalid)",
         len(fact_sales), len(raw_sales_records) - len(fact_sales))

# ── 3d. Build dim_date ─────────────────────────────────────────────────────
log.info("  Building dim_date …")

earliest_sale_date = fact_sales["OrderDate"].min()
latest_sale_date   = fact_sales["OrderDate"].max()
log.info("    Date range: %s → %s", earliest_sale_date.date(), latest_sale_date.date())

full_calendar_date_range = pd.date_range(start=earliest_sale_date, end=latest_sale_date, freq="D")
dim_date = pd.DataFrame({
    "DateKey"    : full_calendar_date_range.strftime("%Y%m%d").astype(int),
    "Date"       : full_calendar_date_range,
    "Year"       : full_calendar_date_range.year,
    "Quarter"    : full_calendar_date_range.quarter,
    "Month"      : full_calendar_date_range.month,
    "MonthName"  : full_calendar_date_range.strftime("%B"),
    "Day"        : full_calendar_date_range.day,
    "DayOfWeek"  : full_calendar_date_range.dayofweek,         # 0=Mon … 6=Sun
    "DayName"    : full_calendar_date_range.strftime("%A"),
    "WeekOfYear" : full_calendar_date_range.isocalendar().week.astype(int),
    "IsWeekend"  : full_calendar_date_range.dayofweek >= 5,
})

log.info("    %s date rows", len(dim_date))

# Add DateKey FK to fact_sales for joining
fact_sales["DateKey"] = fact_sales["OrderDate"].dt.strftime("%Y%m%d").astype(int)

# ── 3e. Build fact_forecast ────────────────────────────────────────────────
log.info("  Building fact_forecast …")

fact_forecast = pd.DataFrame(raw_forecast_records)
fact_forecast.columns = [column_name.strip() for column_name in fact_forecast.columns]
fact_forecast["CountryRegion"] = fact_forecast["CountryRegion"].str.strip()
fact_forecast["Brand"]         = fact_forecast["Brand"].str.strip()
fact_forecast["Forecast"]      = fact_forecast["Forecast"].astype(float)
fact_forecast["Year"]          = fact_forecast["Year"].astype(int)

log.info("    %s forecast rows, years: %s",
         len(fact_forecast), sorted(fact_forecast["Year"].unique()))


# ═══════════════════════════════════════════════════════════════════════════
# STEP 4 – LOAD (export to CSV)
# ═══════════════════════════════════════════════════════════════════════════
log.info("── STEP 4: Loading to CSV ──────────────────────────────────────")

output_tables = {
    "dim_product.csv"   : dim_product,
    "dim_customer.csv"  : dim_customer,
    "dim_date.csv"      : dim_date,
    "fact_sales.csv"    : fact_sales,
    "fact_forecast.csv" : fact_forecast,
}

for output_filename, dataframe_to_export in output_tables.items():
    output_file_path = OUTPUT_DIR / output_filename
    dataframe_to_export.to_csv(output_file_path, index=False, encoding="utf-8-sig")
    log.info("    ✓  %-22s  %7s rows  %s cols", output_filename, f"{len(dataframe_to_export):,}", len(dataframe_to_export.columns))

# ── Summary ────────────────────────────────────────────────────────────────
log.info("── DONE ────────────────────────────────────────────────────────")
log.info("  Total Sales Amount : $%s", f"{fact_sales['SalesAmount'].sum():,.0f}")
log.info("  Years covered      : %s", sorted(fact_sales['Year'].unique()))
log.info("  Unique products    : %s", len(dim_product))
log.info("  Unique customers   : %s", len(dim_customer))
log.info("  Output directory   : %s", OUTPUT_DIR)
