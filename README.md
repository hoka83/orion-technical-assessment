# Project Overview
End-to-end data pipeline and Power BI dashboard analyzing retail sales data (2008–2009) across products, customers, and geographies. The solution covers ETL, data modeling, DAX measures, and interactive visualizations.
________________________________________________________

# ETL Pipeline (etl_orion.py)
How to Run
bash# Install dependencies
pip install ijson pandas numpy
________________________________________________________
# Place Sales.json in data/ folder, then run
python etl_orion.py
What it does
StepDescriptionExtractStreams Sales.json (298K rows) using ijson to avoid memory overloadQuality CheckReports nulls, duplicates, invalid prices/quantitiesTransformBuilds star schema: 3 dims + 2 factsLoadExports 5 UTF-8 CSVs ready for Power BI
Data Quality Issues Handled

218,008 duplicate rows removed (same product/customer/date/qty/price)
CustomerName null in 90% of records — filled with "Unknown" where unresolvable
OrderDate format M/D/YYYY parsed to proper datetime
Invalid prices/quantities (≤ 0) excluded from fact_sales
CustomerCode stored as Text to preserve alphanumeric values (e.g. CSxxx)
________________________________________________________

# Data Model
Star schema with fact_sales at the center:

fact_forecast (linked via TREATAS DAX — no physical relationship)
![Data Model](Data%20model%20Screenshot.png)

________________________________________________________
# Power BI Dashboard
DAX Measures
MeasureDescriptionTotal SalesSUM of SalesAmountSales 2008 / 2009Year-filtered salesYoY Growth %(2009 - 2008) / 2008Total Forecast 2009TREATAS to match Brand without relationshipForecast Achievement %Actual / Forecast ratioTop Customer NameTOPN(1) excluding Unknown
Visuals

KPI Cards: Total Sales, YoY Growth %, Total Quantity, Top Customer
Sales Trend by Month & Year (Line Chart)
2008 vs 2009 Comparison (Clustered Bar)
Top 10 Products by Sales (Bar Chart)
Forecast vs Actual 2009 by Brand (Clustered Bar)
Customer Purchase Behavior (Matrix: Customer x Category)
Slicers: Country, State
![PowerBI Dashboard](dashboard%20Screenshot.png)
________________________________________________________
# Key Assumptions

Duplicate transactions in source data treated as data entry errors and removed


fact_forecast is at Brand x Country x Year granularity — linked to dim_product via TREATAS instead of Many-to-Many relationship to avoid fan-out
Sales.json excluded from repo due to file size (187MB) — run ETL script to regenerate outputs
