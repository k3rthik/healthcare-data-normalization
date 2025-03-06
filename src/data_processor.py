import os
from dotenv import load_dotenv
from pyspark.sql.window import Window
from pyspark.sql.functions import sum as spark_sum, to_date
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    ArrayType,
    DoubleType,
)
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    lit,
    regexp_replace,
    format_number,
    when,
    explode,
    max,
    col,
    sum as spark_sum,
    count,
    round,
    date_format,
    countDistinct,
    first,
    format_number,
    regexp_replace,
)
import glob
import shutil
from typing import Dict
from time_series import ProphetForecaster


class DataProcessor:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.config = None
        self.products_df = None
        self.transactions_df = None
        self.daily_summary_df = None
        self.output_path = "data/output"  # Default output path

    def configure(self, config: Dict) -> None:
        """Configure the data processor with environment settings"""
        self.config = config
        if "output_path" in config:
            self.output_path = config["output_path"]
        # Ensure output directory exists
        os.makedirs(self.output_path, exist_ok=True)
        print("\n✅ Configuration Loaded")

    def load_csv_to_mysql(self, csv_paths, mysql_url, table_names):
        """Load CSV files into MySQL"""
        mysql_user = self.config.get("mysql_user")
        mysql_password = self.config.get("mysql_password")

        for csv_path, table_name in zip(csv_paths, table_names):
            df = self.spark.read.csv(csv_path, header=True, inferSchema=True)
            self._write_to_mysql(df, mysql_url, table_name, mysql_user, mysql_password)
            print(f"✅ Uploaded {csv_path} to MySQL table {table_name}")

    def _write_to_mysql(self, df, mysql_url, table_name, mysql_user, mysql_password):
        """Helper to write a dataframe to MySQL"""
        df.write.format("jdbc").options(
            url=mysql_url,
            driver="com.mysql.cj.jdbc.Driver",
            dbtable=table_name,
            user=mysql_user,
            password=mysql_password,
        ).mode("overwrite").save()

    def load_json_to_mongodb(self, json_paths, date_range):
        """Load JSON transaction files into MongoDB collections"""
        mongodb_uri = self.config.get("mongodb_uri")
        mongodb_db = self.config.get("mongodb_db")
        schema = self._get_transaction_schema()

        for json_path, date in zip(json_paths, date_range):
            collection_name = f"transactions_{date}"
            print(f"📥 Processing {json_path} → MongoDB Collection: {collection_name}")

            if not os.path.exists(json_path):
                print(f"⚠️ JSON file not found: {json_path}")
                continue

            df = self._prepare_transaction_df(json_path, schema)
            self._write_to_mongodb(df, mongodb_uri, mongodb_db, collection_name)
            print(f"✅ Uploaded {json_path} to {collection_name}")

    def _get_transaction_schema(self):
        """Create schema for transaction data"""
        return StructType(
            [
                StructField("transaction_id", IntegerType(), True),
                StructField("customer_id", IntegerType(), True),
                StructField("timestamp", StringType(), True),
                StructField(
                    "items",
                    ArrayType(
                        StructType(
                            [
                                StructField("product_id", IntegerType(), True),
                                StructField("product_name", StringType(), True),
                                StructField("qty", IntegerType(), True),
                            ]
                        )
                    ),
                    True,
                ),
            ]
        )

    def _prepare_transaction_df(self, json_path, schema):
        """Read and prepare transaction dataframe"""
        df = self.spark.read.schema(schema).json(json_path)

        return (
            df.select(
                "transaction_id",
                "customer_id",
                "timestamp",
                explode(col("items")).alias("item"),
            )
            .withColumn("product_id", col("item.product_id"))
            .withColumn("product_name", col("item.product_name"))
            .withColumn("qty", col("item.qty"))
        )

    def _write_to_mongodb(self, df, mongodb_uri, mongodb_db, collection_name):
        """Write dataframe to MongoDB"""
        df.write.format("mongo").mode("append").options(
            uri=f"{mongodb_uri}/{mongodb_db}", collection=collection_name
        ).save()

    def generate_orders(self):
        """Generate orders.csv"""
        if not self._check_data_availability():
            return

        orders_df = self._create_orders_dataframe()
        self._save_as_csv(orders_df, "orders.csv")
        print(f"✅ Generated {os.path.join(self.output_path, 'orders.csv')}")

    def _create_orders_dataframe(self):
        """Transform transactions into the orders table"""
        # Step 1: Generate order line items first (ensuring valid items only)
        order_items_df = self._create_orders_line__dataframe()

        # Step 2: Aggregate order-level data
        orders_df = (
            order_items_df.groupBy("order_id")
            .agg(
                format_number(spark_sum(col("line_total")), 2).alias(
                    "total_amount"
                ),  # Compute total price per order
                count(col("product_id")).alias(
                    "num_items"
                ),  # Count distinct products per order
            )
            .orderBy("order_id")
        )  # Ensure sorting

        # Step 3: Join with original transaction data to get order timestamps and customer IDs
        orders_df = orders_df.join(
            self.transactions_df.select(
                col("transaction_id").alias("order_id"),
                col("timestamp").alias("order_datetime"),
                col("customer_id"),
            ).distinct(),
            on="order_id",
            how="left",
        )

        # Step 4: Select final columns and return
        return orders_df.select(
            col("order_id"),
            col("order_datetime"),
            col("customer_id"),
            col("total_amount"),
            col("num_items"),
        ).orderBy("order_id")

    def _check_data_availability(self):
        """Check if transaction and product data is available"""
        if self.transactions_df is None or self.products_df is None:
            print("⚠️ No transaction or product data found!")
            return False
        return True

    def _create_orders_line__dataframe(self):
        """Transform transaction data into order_line_items table with stock validation"""
        # Step 1: Explode transaction items and order by order_id, product_id
        order_items_df = (
            self.transactions_df.select(
                col("transaction_id").alias("order_id"),
                col("timestamp"),
                explode(col("items")).alias("item"),
            )
            .select(
                col("order_id"),
                col("timestamp"),
                col("item.product_id"),
                col("item.qty").alias("quantity"),
            )
            .orderBy("order_id", "product_id")
        )  # Ensure sorting

        # Step 2: Remove order items where quantity is NULL
        order_items_df = order_items_df.filter(col("quantity").isNotNull())

        # Step 3: Join with products_df to get unit_price and stock
        order_items_df = order_items_df.join(
            self.products_df.select(
                col("product_id"), col("sales_price").alias("unit_price"), col("stock")
            ),
            on="product_id",
            how="left",
        )

        # Step 4: Convert stock values to a dictionary for sequential tracking
        stock_tracker = self.products_df.select(
            "product_id", "stock"
        ).rdd.collectAsMap()

        # Step 5: Define schema to prevent PySpark type mismatch
        schema = StructType(
            [
                StructField("order_id", IntegerType(), False),
                StructField("product_id", IntegerType(), False),
                StructField("quantity", IntegerType(), False),
                StructField("unit_price", DoubleType(), False),
                StructField("line_total", DoubleType(), False),
            ]
        )

        # Step 6: Apply stock validation and update stock dynamically
        valid_items = []
        for row in order_items_df.collect():  # Process in order
            product_id = row["product_id"]
            order_id = row["order_id"]
            requested_qty = int(row["quantity"])  # Ensure IntegerType
            unit_price = float(row["unit_price"])  # Ensure DoubleType

            # Ensure stock is an integer and default to 0 if missing
            available_stock = stock_tracker.get(product_id, 0)

            if available_stock >= requested_qty:
                stock_tracker[product_id] -= requested_qty  # Deduct stock correctly
                line_total = requested_qty * unit_price
            else:
                # Set quantity and total to zero if stock is insufficient
                requested_qty = 0
                line_total = float(
                    0
                )  # **Fix: Ensure line_total is explicitly a float**

            # Append valid item with correct types
            valid_items.append(
                (order_id, product_id, requested_qty, unit_price, line_total)
            )

        # Convert valid order items to a PySpark DataFrame with defined schema
        processed_order_items_df = self.spark.createDataFrame(
            valid_items, schema=schema
        )

        # Step 7: Fix the `.fillna()` issue by replacing column reference with a literal value
        processed_order_items_df = processed_order_items_df.fillna(
            {"quantity": 0, "line_total": 0.0}
        )

        # Step 8: Format line_total properly using PySpark function
        return processed_order_items_df.select(
            col("order_id"),
            col("product_id"),
            col("quantity"),
            format_number(col("unit_price"), 2).alias("unit_price"),
            format_number(col("line_total"), 2).alias(
                "line_total"
            ),  # Handling rounding here
        ).orderBy("order_id", "product_id")

    def _explode_transactions(self):
        """Explode transaction items into separate rows"""
        exploded_df = self.transactions_df.select(
            col("transaction_id").alias("order_id"),
            col("timestamp").alias("order_datetime"),
            col("customer_id"),
            explode(col("items")).alias("item"),
        ).select(
            col("order_id"),
            col("order_datetime"),
            col("customer_id"),
            col("item.product_id"),
            col("item.qty").alias("quantity"),
        )
        # Debug: Print all rows in the exploded DataFrame
        print("DEBUG: Exploded Transactions Data:")
        exploded_df.show(exploded_df.count(), truncate=False)
        return exploded_df

    def _join_with_products(self, df):
        """Join dataframe with product information"""
        joined_df = df.join(
            self.products_df.select(
                col("product_id"),
                col("sales_price").alias("unit_price"),  # Explicitly renaming
            ),
            on="product_id",
            how="left",
        )

    def generate_order_line_items(self):
        """Generate order_line_items.csv with stock validation"""
        if not self._check_data_availability():
            return

        order_items_df = self._create_orders_line__dataframe()
        self._save_as_csv(order_items_df, "order_line_items.csv")
        print(f"✅ Generated {os.path.join(self.output_path, 'order_line_items.csv')}")

        # Step 4: Convert stock values to a dictionary for sequential tracking
        stock_tracker = self.products_df.select(
            "product_id", "stock"
        ).rdd.collectAsMap()

        # Step 5: Define schema to prevent PySpark type mismatch
        schema = StructType(
            [
                StructField("order_id", IntegerType(), False),
                StructField("product_id", IntegerType(), False),
                StructField("quantity", IntegerType(), False),
                StructField("unit_price", DoubleType(), False),
                StructField("line_total", DoubleType(), False),
            ]
        )

        # Step 6: Apply stock validation and update stock dynamically
        valid_items = []
        for row in order_items_df.collect():  # Process in order
            product_id = row["product_id"]
            order_id = row["order_id"]
            requested_qty = int(row["quantity"])  # Ensure IntegerType
            unit_price = float(row["unit_price"])  # Ensure DoubleType

            # Ensure stock is an integer and default to 0 if missing
            available_stock = stock_tracker.get(product_id, 0)

            if available_stock >= requested_qty:
                stock_tracker[product_id] -= requested_qty  # Deduct stock correctly
                line_total = requested_qty * unit_price
            else:
                # Set quantity and total to zero if stock is insufficient
                requested_qty = 0
                line_total = float(
                    0
                )  # **Fix: Ensure line_total is explicitly a float**

                print(
                    f"❌ Item {product_id} in Order {order_id} was canceled due to insufficient stock."
                )

            # Append valid item with correct types
            valid_items.append(
                (order_id, product_id, requested_qty, unit_price, line_total)
            )

        # Convert valid order items to a PySpark DataFrame with defined schema
        processed_order_items_df = self.spark.createDataFrame(
            valid_items, schema=schema
        )

        # Step 7: Format line_total properly using PySpark function
        return processed_order_items_df.select(
            col("order_id"),
            col("product_id"),
            col("quantity"),
            col("unit_price"),
            format_number(col("line_total"), 2).alias(
                "line_total"
            ),  # Handling rounding here
        ).orderBy("order_id", "product_id")

    def update_inventory(self):
        """Update inventory and generate products_updated.csv"""
        if not self._check_data_availability():
            return

        # Ensure order line items data is available
        order_items_df = self._create_orders_line__dataframe()  # Use the correct method

        updated_inventory_df = self._create_updated_inventory_dataframe(order_items_df)

        if updated_inventory_df is None or updated_inventory_df.count() == 0:
            print("⚠️ No valid inventory updates found. Skipping file generation.")
            return  # Stop execution if no data

        self._save_as_csv(updated_inventory_df, "products_updated.csv")
        print(f"✅ Generated {os.path.join(self.output_path, 'products_updated.csv')}")

    def _create_updated_inventory_dataframe(self, order_items_df):
        """Create updated inventory dataframe using order_line_items.csv"""
        # Ensure order_items_df is valid before proceeding
        if order_items_df is None or order_items_df.count() == 0:
            print("⚠️ No valid order line items found. Returning empty DataFrame.")
            return self.spark.createDataFrame(
                [], self.products_df.schema
            )  # Return empty DataFrame instead of None

        # Step 1: Compute total sold items per product using order_line_items.csv
        sold_items = order_items_df.groupBy("product_id").agg(
            spark_sum(col("quantity")).alias(
                "total_sold"
            )  # Using quantity instead of num_items
        )

        # Step 2: Join with products to get updated stock
        updated_inventory_df = (
            self.products_df.join(sold_items, "product_id", "left_outer")
            .fillna(0, ["total_sold"])
            .withColumn(
                "updated_stock",
                when((col("stock") - col("total_sold")) < 0, 0).otherwise(
                    col("stock") - col("total_sold")
                ),
            )
            .select(
                "product_id",
                "product_name",
                col("updated_stock").alias("current_stock"),
            )
            .orderBy("product_id")
        )

        return updated_inventory_df

    def _calculate_sold_items(self):
        """Calculate total sold items per product"""
        return (
            self.transactions_df.select(explode(col("items")).alias("item"))
            .select(
                col("item.product_id").alias("product_id"),
                col("item.qty").alias("sold_qty"),
            )
            .groupBy("product_id")
            .agg(spark_sum("sold_qty").alias("total_sold"))
        )

    def aggregate_daily_sales(self):
        """Generate daily_summary.csv and store it in MySQL"""
        if not self._check_data_availability():
            return

        print("\n📊 Transactions Data Sample BEFORE Aggregation:")
        self.transactions_df.show(5)  # Debugging step

        self.daily_summary_df = self._create_daily_summary_dataframe()

        if self.daily_summary_df is None or self.daily_summary_df.count() == 0:
            print("⚠️ No valid daily sales data found. Skipping file generation.")
            return  # Stop execution if no data

        self._save_as_csv(self.daily_summary_df, "daily_summary.csv")
        print(f"✅ Generated {os.path.join(self.output_path, 'daily_summary.csv')}")

        # Save to MySQL
        mysql_url = self.config["mysql_url"]
        mysql_user = self.config["mysql_user"]
        mysql_password = self.config["mysql_password"]

        self._write_to_mysql(
            self.daily_summary_df,
            mysql_url,
            "daily_summary",
            mysql_user,
            mysql_password,
        )
        print("✅ Stored daily_summary in MySQL")

    def _create_orders_with_products_dataframe(self):
        """Creates an orders DataFrame that includes product_id for joining with product costs"""

        # Step 1: Generate order line items (which contain product_id)
        order_items_df = self.generate_order_line_items()

        # Step 2: Generate orders DataFrame
        orders_df = self._create_orders_dataframe()

        # Step 3: Join order line items with orders to get product_id associated with each order_id
        orders_with_products_df = order_items_df.join(
            orders_df, on="order_id", how="left"
        ).select(
            order_items_df["order_id"],
            order_items_df["product_id"],
            orders_df["order_datetime"],
            orders_df["total_amount"],
            orders_df["num_items"],
        )

        return orders_with_products_df

    def _create_daily_summary_dataframe(self):
        """Generate the daily summary DataFrame using orders and product costs"""

        # Step 1: Generate order line items
        order_line_items_df = self.generate_order_line_items()

        # Step 2: Generate orders DataFrame
        orders_df = self._create_orders_dataframe()

        # Step 3: Join order line items with products to get cost price
        order_line_items_profit_df = order_line_items_df.join(
            self.products_df.select("product_id", "cost_to_make"),
            on="product_id",
            how="left",
        ).withColumn(
            "profit",
            (col("unit_price").cast("double") - col("cost_to_make").cast("double"))
            * col("quantity"),
        )

        # Step 4: Aggregate profit per order
        order_profit_df = order_line_items_profit_df.groupBy("order_id").agg(
            spark_sum("profit").alias("profit")
        )

        # Step 5: Join profit data with orders
        orders_with_profit_df = orders_df.join(
            order_profit_df, on="order_id", how="left"
        )

        # Step 6: Prepare formatted orders DataFrame
        formatted_orders_df = orders_with_profit_df.select(
            col("order_id"),
            col("order_datetime"),
            col("customer_id"),
            col("total_amount"),
            col("profit"),
        ).orderBy("order_datetime")

        # Step 7: Convert `order_datetime` to `date` format
        daily_summary_df = (
            formatted_orders_df.withColumn(
                "date",
                date_format(col("order_datetime").cast("timestamp"), "yyyy-MM-dd"),
            )
            .groupBy("date")
            .agg(
                count("order_id").alias("num_orders"),
                regexp_replace(
                    format_number(spark_sum(col("total_amount")), 2), ",", ""
                ).alias("total_sales"),
                round(spark_sum("profit"), 2).alias("total_profit"),
            )
            .orderBy("date")
        )

        return daily_summary_df

    def _prepare_daily_sales_data(self):
        """Prepare data for daily sales aggregation"""
        daily_sales_df = self.transactions_df.select(
            col("transaction_id"),
            col("customer_id"),
            explode(col("items")).alias("item"),
            col("timestamp"),
        ).select(
            col("transaction_id"),
            col("customer_id"),
            col("item.product_id"),
            col("item.qty"),
            col("timestamp"),
        )

        return (
            daily_sales_df.join(
                self.products_df.select("product_id", "sales_price", "cost_to_make"),
                on="product_id",
                how="left",
            )
            .withColumnRenamed("sales_price", "unit_price")
            .withColumnRenamed("cost_to_make", "cost_price")
        )

    def _save_as_csv(self, df, filename):
        """Save dataframe as CSV with specified filename"""
        # Create output directory path
        output_file = os.path.join(self.output_path, filename.replace(".csv", ""))

        # Delete directory if it exists (to avoid conflicts)
        if os.path.exists(output_file):
            shutil.rmtree(output_file, ignore_errors=True)

        # Write as a single file with header
        df = df.dropna(how="any")  # Drop columns with only NULL values
        df.coalesce(1).write.option("header", "true").csv(output_file)

        # Rename the part file to the desired filename
        self._rename_part_file(output_file, filename)

    def _rename_part_file(self, directory, target_filename):
        """Rename the part-00000 file to the target filename"""
        # Find the part file
        part_files = glob.glob(f"{directory}/part-00000-*.csv")
        if not part_files:
            print(f"⚠️ Warning: No part file found in {directory}")
            return

        # Get first part file
        part_file = part_files[0]

        # Create the target filepath
        target_path = os.path.join(self.output_path, target_filename)

        # Rename the file
        try:
            shutil.copy2(part_file, target_path)

            # Clean up the part file directory
            shutil.rmtree(directory)
        except Exception as e:
            print(f"⚠️ Error renaming file: {e}")

    def generate_sales_profit_forecast(self):
        """Generate sales_profit_forecast.csv using time series forecasting"""

        # Ensure daily_summary exists
        daily_summary_path = os.path.join(self.output_path, "daily_summary.csv")
        if not os.path.exists(daily_summary_path):
            print("⚠️ No daily summary data found! Skipping forecasting.")
            return

        # Load daily_summary.csv
        daily_summary_df = self.spark.read.csv(
            daily_summary_path, header=True, inferSchema=True
        )

        # Prepare data for forecasting
        sales_data = (
            daily_summary_df.select("total_sales")
            .rdd.map(lambda row: float(row["total_sales"]))
            .collect()
        )
        profit_data = (
            daily_summary_df.select("total_profit")
            .rdd.map(lambda row: float(row["total_profit"]))
            .collect()
        )

        # Initialize forecasting model for total sales
        sales_forecaster = ProphetForecaster()  # Create a new instance for sales
        sales_forecaster.fit(sales_data)  # Fit Prophet model on total sales data
        sales_forecast = sales_forecaster.predict(forecast_days=1)[
            0
        ]  # Forecast for 1 day (Feb 11th, 2024)

        # Initialize forecasting model for total profit
        profit_forecaster = ProphetForecaster()  # Create a new instance for profit
        profit_forecaster.fit(profit_data)  # Fit Prophet model on total profit data
        profit_forecast = profit_forecaster.predict(forecast_days=1)[
            0
        ]  # Forecast for 1 day (Feb 11th, 2024)

        # Prepare the data in the expected format for Spark
        forecast_data = [("2024-02-11", float(sales_forecast), float(profit_forecast))]

        # Define the schema for the DataFrame
        forecast_schema = StructType(
            [
                StructField("date", StringType(), True),
                StructField("forecasted_sales", DoubleType(), True),
                StructField("forecasted_profit", DoubleType(), True),
            ]
        )

        # Create a Spark DataFrame with the forecasted data
        forecast_df = self.spark.createDataFrame(forecast_data, schema=forecast_schema)

        # Save the forecast results to CSV
        self._save_as_csv(forecast_df, "sales_profit_forecast.csv")
        print(
            f"✅ Generated {os.path.join(self.output_path, 'sales_profit_forecast.csv')}"
        )

    def run(self):
        """Run the entire processing pipeline"""
        self.generate_order_line_items()
        self.generate_orders()
        self.update_inventory()
        self.aggregate_daily_sales()
        self.generate_sales_profit_forecast()
        print("✅ All processing steps completed successfully!")
