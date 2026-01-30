## Cubix Data Engineer Capstone

End‑to‑end data engineering capstone that ingests AdventureWorks‑style sales data into a Medallion architecture (Bronze/Silver/Gold) on Databricks using PySpark and Delta Lake. The project focuses on dimensional modelling, SCD handling and building reusable, testable ETL logic.

---

### What This Project Does

- Ingests raw CSV/Parquet data from Unity Catalog volumes into a **Bronze** layer.
- Cleans and models dimensions and facts into a **Silver** layer (calendar, customers, products, product categories, product subcategories, sales).
- Applies **SCD Type 1** logic on Delta tables for master data maintenance.
- Builds a **wide sales fact** table by joining sales with all dimensions.
- Produces **Gold**-layer aggregates:
	- Daily sales metrics (sums/averages of sales amount and profit).
	- Product category‑level sales metrics.

All transformations are implemented as small, testable PySpark functions with unit tests.

---

### Technologies Used

- **Language**: Python (>= 3.11)
- **Compute**: Apache Spark / PySpark (3.5.7)
- **Storage & Lakehouse**:
	- Databricks Unity Catalog Volumes
	- Delta Lake (via `delta-spark` optional dependency)
- **Data Formats**: CSV, Parquet, Delta
- **Testing**: pytest, `pyspark.testing` utilities

Optional extras (for local/dev use):

- **pyarrow**, **pandas**, **pre-commit** hooks (configured in `pyproject.toml`).

---

### Concepts Applied

- **Medallion Architecture**
	- **Bronze**: Raw ingestion from source volumes, minimal transformation.
	- **Silver**: Cleansed and conformed dimensions/facts (casting, renaming, deduplication).
	- **Gold**: Curated marts and aggregates for reporting (daily metrics and category metrics).

- **Slowly Changing Dimensions (SCD)**
	- **SCD Type 1** implementation for Unity Catalog Delta tables in
		[cubix_data_engineer_capstone/etl/silver/scd.py](cubix_data_engineer_capstone/etl/silver/scd.py)
		using Delta Lake `merge` semantics.

- **Dimensional Modelling**
	- Fact table: sales (order line level), enriched into a wide fact in
		[cubix_data_engineer_capstone/etl/gold/wide_sales.py](cubix_data_engineer_capstone/etl/gold/wide_sales.py).
	- Dimensions: calendar, customer, product, product subcategory, product category.

- **Data Quality & Testing**
	- Unit tests validate schema, transformations and aggregations in `tests/etl/silver` and `tests/etl/gold`.

---

### Project Structure

Top‑level layout:

- [cubix_data_engineer_capstone](cubix_data_engineer_capstone)
	- [etl](cubix_data_engineer_capstone/etl)
		- [bronze](cubix_data_engineer_capstone/etl/bronze)
			- `extract_and_load_file.py` – `bronze_ingest_volume` reads raw files from UC volumes using Spark and writes them as Bronze datasets.
		- [silver](cubix_data_engineer_capstone/etl/silver)
			- `calendar.py` – builds calendar/date dimension.
			- `customers.py` – cleans and models customer dimension (including marital status / gender encoding).
			- `products.py` – cleans product dimension.
			- `product_subcategory.py` – subcategory dimension transformations.
			- `product_category.py` – category dimension transformations.
			- `sales.py` – `get_sales` builds a clean sales fact from raw feed.
			- `scd.py` – `scd1_uc` helper for SCD1 merges into Delta tables in Unity Catalog.
		- [gold](cubix_data_engineer_capstone/etl/gold)
			- `wide_sales.py` – joins all master tables to create a wide sales fact and derives `SalesAmount`, `HighValueOrder`, and `Profit`.
			- `daily_sales_metrics.py` – `get_daily_sales_metrics` computes daily sums/averages for sales amount and profit.
			- `daily_product_category_metrics.py` – `get_daily_product_category_metrics` computes category‑level sums/averages for sales amount and profit.
	- [utils](cubix_data_engineer_capstone/utils)
		- `databricks.py` – utilities to read/write CSV/Parquet/Delta from/to Unity Catalog volumes with PySpark.

- [tests](tests)
	- [etl/silver](tests/etl/silver)
		- Tests for `calendar`, `customers`, `product_category`, `product_subcategory`, `sales` transformations.
	- [etl/gold](tests/etl/gold)
		- Tests for `wide_sales`, `daily_sales_metrics`, `daily_product_category_metrics`.

- [pyproject.toml](pyproject.toml)
	- Defines dependencies, optional `delta` extra, and pytest configuration.

---

### Installing Dependencies

This project uses `pyproject.toml` for dependency management (Poetry style). You can either use Poetry or install directly with `pip`.

**Using Poetry (recommended)**

1. Install Poetry (if needed):
	 - `pip install poetry`
2. From the project root (where `pyproject.toml` lives), run:
	 - `poetry install`         # base dependencies
	 - `poetry install -E delta`  # if you need Delta Lake support

**Using pip directly**

From the project root:

- Install core runtime deps:
	- `pip install "numpy>=1" "pyspark==3.5.7"`
- Install optional extras (Delta + dev/test tooling):
	- `pip install "delta-spark>=3.3.0" pytest pyarrow pandas pre-commit`

If you prefer a `requirements.txt`, you can export from Poetry:

- `poetry export -f requirements.txt --output requirements.txt --without-hashes`

---

### Running on Databricks

You can run the transformations directly in a Databricks Notebook, Job, or Workflow.

**1. Cluster / Warehouse setup**

- Runtime with:
	- Python >= 3.11
	- Spark 3.5.x
	- Delta Lake (Databricks runtimes already include this).
- Attach a catalog and schema with Unity Catalog enabled.

**2. Install the project code**

Option A – **Workspace import**

- Import the project as a repo into Databricks Repos (e.g. from GitHub).
- Ensure the repo path is on `sys.path` (Databricks handles this automatically for Repos).

Option B – **Wheel / package**

- Build a wheel using Poetry locally, then upload and install it on the cluster.

**3. Configure Unity Catalog volumes and paths**

- Create UC volumes for raw, bronze, silver, and gold layers (example):
	- `/Volumes/<catalog>/<schema>/raw`
	- `/Volumes/<catalog>/<schema>/bronze`
	- `/Volumes/<catalog>/<schema>/silver`
	- `/Volumes/<catalog>/<schema>/gold`

**4. Example notebook flow**

```python
from pyspark.sql import SparkSession

from cubix_data_engineer_capstone.etl.bronze.extract_and_load_file import bronze_ingest_volume
from cubix_data_engineer_capstone.utils.databricks import read_file_from_volume, write_file_to_volume
from cubix_data_engineer_capstone.etl.silver.sales import get_sales
from cubix_data_engineer_capstone.etl.gold.wide_sales import get_wide_sales
from cubix_data_engineer_capstone.etl.gold.daily_sales_metrics import get_daily_sales_metrics

spark = SparkSession.getActiveSession()

raw_path = "/Volumes/<catalog>/<schema>/raw"
bronze_path = "/Volumes/<catalog>/<schema>/bronze"
silver_path = "/Volumes/<catalog>/<schema>/silver"
gold_path = "/Volumes/<catalog>/<schema>/gold"

# 1) Bronze ingestion
bronze_ingest_volume(raw_path, bronze_path, "sales.csv", partition_by=["orderdate"])

# 2) Silver sales
sales_bronze = read_file_from_volume(f"{bronze_path}/sales.csv", "csv")
sales_silver = get_sales(sales_bronze)
write_file_to_volume(sales_silver, f"{silver_path}/sales", "delta", mode="overwrite")

# 3) Build wide sales (requires also loading calendar, customers, products, etc.)
# calendar_silver = read_file_from_volume(f"{silver_path}/calendar", "delta")
# ... load other silver masters ...
# wide_sales = get_wide_sales(sales_silver, calendar_silver, customers_silver, products_silver, subcat_silver, cat_silver)
# write_file_to_volume(wide_sales, f"{gold_path}/wide_sales", "delta", mode="overwrite")

# 4) Daily sales metrics
# daily_metrics = get_daily_sales_metrics(wide_sales)
# write_file_to_volume(daily_metrics, f"{gold_path}/daily_sales_metrics", "delta", mode="overwrite")
```

Adapt the paths, file names, and loading of silver master tables to match your environment.

---

### How to Run Tests

Tests are standard pytest tests and use PySpark.

**Prerequisites**

- All dev/test dependencies installed (via `poetry install` or `pip`).
- A local Spark environment matching the PySpark version (3.5.7).

**Running tests locally**

From the project root:

- With Poetry:
	- `poetry run pytest`

- With plain pip / virtualenv:
	- `pytest`

The pytest configuration (test paths, warning filters) is defined in
[pyproject.toml](pyproject.toml).

---

### Notes

- The package name is `cubix_data_engineer_capstone`; import modules using that root.
- Delta Lake features (such as SCD1 merges) require the `delta-spark` extra and a Delta‑enabled Spark runtime (e.g. Databricks).
