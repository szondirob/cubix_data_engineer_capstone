"""Build a wide sales fact DataFrame by joining master tables and deriving metrics.

This module provides helpers to:
- Join cleaned sales, calendar, customer, product, subcategory, and category
  master tables into a single wide DataFrame.
- Enrich the joined data with human‑readable attributes (e.g. marital status,
  gender).
- Compute key measures such as SalesAmount, HighValueOrder flags, and Profit
  for downstream reporting and aggregations.
"""

import pyspark.sql.functions as sf
from pyspark.sql import DataFrame


def _join_master_tables(
        sales_master: DataFrame,
        calendar_master: DataFrame,
        customer_master: DataFrame,
        products_master: DataFrame,
        product_subcategory_master: DataFrame,
        product_category_master: DataFrame
) -> DataFrame:
    """
    Join the master DataFrames to the Sales Master.
    Drop Date, ProductSubCategoryKey and ProductCategoryKey to avoid duplicate columns.

    :param sales_master:                Master DataFrames for Sales.
    :param calendar_master:             Master DataFrames for Calendar.
    :param customer_master:             Master DataFrames for Customer.
    :param products_master:             Master DataFrames for Products.
    :param product_subcategory_master:  Master DataFrames for Product Subcategory.
    :param product_category_master:     Master DataFrames for Product Category.
    :return:                            Sales Master with all the joined DataFrames.
    """

    return (
        sales_master
        .join(calendar_master, sales_master["OrderDate"] == calendar_master["Date"], how="left")
        .drop(calendar_master["Date"])
        .join(customer_master, on="CustomerKey", how="left")
        .join(products_master, on="ProductKey", how="left")
        .join(
            product_subcategory_master,
            products_master["ProductSubCategoryKey"] == product_subcategory_master["ProductSubCategoryKey"],
            how="left"
        )
        .drop(products_master["ProductSubCategoryKey"])
        .join(
            product_category_master,
            product_category_master["ProductCategoryKey"] == product_subcategory_master["ProductCategoryKey"],
            how="left"
        )
        .drop(product_subcategory_master["ProductCategoryKey"])
    )

def get_wide_sales(
        sales_master: DataFrame,
        calendar_master: DataFrame,
        customer_master: DataFrame,
        products_master: DataFrame,
        product_subcategory_master: DataFrame,
        product_category_master: DataFrame
) -> DataFrame:
    """
    1. Join the Master tables.
    2. Convert the MaritalStatus and Gender to human readable format.
    2. Calculate SalesAmount, HighValueOrder, Profit.

    :param sales_master:                Master DataFrames for Sales.
    :param calendar_master:             Master DataFrames for Calendar.
    :param customer_master:             Master DataFrames for Customer.
    :param products_master:             Master DataFrames for Products.
    :param product_subcategory_master:  Master DataFrames for Product Subcategory.
    :param product_category_master:     Master DataFrames for Product Category.
    :return:                            The joined DataFrame with the additional columns.
    """

    wide_sales_df = _join_master_tables(
        sales_master,
        calendar_master,
        customer_master,
        products_master,
        product_subcategory_master,
        product_category_master
    )

    calculate_sales_amount = sf.col("OrderQuantity") * sf.col("ListPrice")
    calculate_high_value_order = sf.col("SalesAmount") > 10000
    calculate_profit = sf.col("SalesAmount") - (sf.col("StandardCost") * sf.col("OrderQuantity"))

    return (
        wide_sales_df
        .withColumn(
            "MaritalStatus",
            sf.when(sf.col("MaritalStatus") == 1, "Married")
            .when(sf.col("MaritalStatus") == 0, "Single")
            .otherwise(None)
        )
        .withColumn(
            "Gender",
            sf.when(sf.col("Gender") == 1, "Male")
            .when(sf.col("Gender") == 0, "Female")
            .otherwise(None)
        )
        .withColumn("SalesAmount", calculate_sales_amount)
        .withColumn("HighValueOrder", calculate_high_value_order)
        .withColumn("Profit", calculate_profit)
    )
