from pyspark.sql import SparkSession, DataFrame
from data_processor import DataProcessor
from dotenv import load_dotenv
from datetime import datetime, timedelta
import os
from pyspark.sql.functions import col
from typing import Dict, Tuple
import traceback
from time_series import ProphetForecaster


def create_spark_session(app_name: str = "CandyStoreAnalytics") -> SparkSession:
    """Create and configure Spark session with MongoDB and MySQL connectors"""
    return (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.jars.packages",
            "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,mysql:mysql-connector-java:8.0.33",
        )
        .config("spark.mongodb.input.uri", os.getenv("MONGODB_URI"))
        .config("spark.mongodb.output.uri", f"{os.getenv('MONGODB_URI')}/{os.getenv('MONGO_DB')}")
        .getOrCreate()
    )


def get_date_range(start_date: str, end_date: str) -> list[str]:
    """Generate a list of dates between start and end date"""
    start = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")
    return [(start + timedelta(days=i)).strftime("%Y%m%d") for i in range((end - start).days + 1)]


def setup_configuration() -> Tuple[Dict, list]:
    """Setup application configuration"""
    load_dotenv()
    config = load_config()
    date_range = get_date_range(
        os.getenv("MONGO_START_DATE"), os.getenv("MONGO_END_DATE")
    )
    return config, date_range


def load_config() -> Dict:
    """Load configuration from environment variables"""
    return {
        "mongodb_uri": os.getenv("MONGODB_URI"),
        "mongodb_db": os.getenv("MONGO_DB"),
        "mysql_url": os.getenv("MYSQL_URL"),
        "mysql_user": os.getenv("MYSQL_USER"),
        "mysql_password": os.getenv("MYSQL_PASSWORD"),
        "customers_table": os.getenv("CUSTOMERS_TABLE"),
        "products_table": os.getenv("PRODUCTS_TABLE"),
        "output_path": os.getenv("OUTPUT_PATH"),
    }


def load_dataframes(spark: SparkSession, config: dict, date_range: list):
    """Load data from MySQL and MongoDB into PySpark DataFrames"""
    print("\n🔄 Loading MySQL Data into PySpark...")

    mysql_url = config["mysql_url"]
    mysql_properties = {
        "user": config["mysql_user"],
        "password": config["mysql_password"],
        "driver": "com.mysql.cj.jdbc.Driver",
    }

    customers_df = spark.read.jdbc(
        url=mysql_url, table=config["customers_table"], properties=mysql_properties
    )
    print("✅ Loaded Customers Table")

    products_df = spark.read.jdbc(
        url=mysql_url, table=config["products_table"], properties=mysql_properties
    )
    print("✅ Loaded Products Table")

    print("\n🔄 Loading MongoDB Data into PySpark...")
    transactions_dfs = []
    for date in date_range:
        collection_name = f"transactions_{date}"
        print(f"📥 Loading MongoDB Collection: {collection_name}")

        try:
            transactions_df = (
                spark.read.format("mongo")
                .option("uri", config["mongodb_uri"])
                .option("database", config["mongodb_db"])
                .option("collection", collection_name)
                .load()
            )
            if transactions_df.count() > 0:
                transactions_dfs.append(transactions_df)
                print(f"✅ Loaded {collection_name}")
            else:
                print(f"⚠️ Warning: {collection_name} is empty. Skipping.")
        except Exception as e:
            print(f"⚠️ Warning: Could not load {collection_name}: {str(e)}")

    # Combine all transaction dataframes into a single DataFrame
    if transactions_dfs:
        transactions_df = transactions_dfs[0]
        for df in transactions_dfs[1:]:
            transactions_df = transactions_df.union(df)
    else:
        transactions_df = None

    return customers_df, products_df, transactions_df


def main():
    print("\nStarting Candy Store Data Processing System")
    print("=" * 80)

    config, date_range = setup_configuration()
    spark = create_spark_session()
    processor = DataProcessor(spark)

    csv_files = [
        "data/dataset_18/customers.csv",
        "data/dataset_18/products.csv"
    ]
    json_files = [
        f"data/dataset_18/transactions_{date}.json" for date in date_range
    ]

    mysql_url = os.getenv("MYSQL_URL")
    table_names = ["customers", "products"]

    try:
        processor.configure(config)
        processor.load_csv_to_mysql(csv_files, mysql_url, table_names)

        if json_files:
            processor.load_json_to_mongodb(json_files, date_range)
        else:
            print("⚠️ Warning: No JSON transaction files found for loading.")

        print("✅ Data successfully loaded into MySQL & MongoDB!")

        customers_df, products_df, transactions_df = load_dataframes(spark, config, date_range)

        # Debugging: Print schema to check if transaction_id exists
        print("\n🔍 Transactions Schema:")
        transactions_df.printSchema()

        # Ensure transactions are properly loaded
        if transactions_df is None or transactions_df.count() == 0:
            print("⚠️ No transaction data found. Skipping processing.")
            return

        processor.products_df = products_df
        processor.transactions_df = transactions_df  # Now includes all transaction files

        processor.run()
        forecaster = ProphetForecaster()
        forecast_input = os.path.join(config["output_path"], "daily_summary.csv")
        forecast_output = os.path.join(config["output_path"], "sales_profit_forecast.csv")
        forecaster.run(mysql_url, config["mysql_user"], config["mysql_password"])

        print("\n✅ All processing completed successfully!")

    except Exception as e:
        print(f"Error occurred: {e}")
        traceback.print_exc()

    finally:
        print("\nCleaning up...")
        spark.stop()


if __name__ == "__main__":
    main()