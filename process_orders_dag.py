import sys
sys.path.append("/Users/karthikpachabatla/project-2/src")

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os
from data_processor import DataProcessor
from time_series import ProphetForecaster
import traceback

# Load environment variables from .env file
load_dotenv("/Users/karthikpachabatla/project-2/.env")

# Configure logging
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get environment variables
mysql_jar = os.getenv("MYSQL_CONNECTOR_PATH")
output_path = os.getenv("OUTPUT_PATH")
mongodb_uri = os.getenv("MONGODB_URI")
mongodb_db = os.getenv("MONGO_DB")
mysql_url = os.getenv("MYSQL_URL")
mysql_user = os.getenv("MYSQL_USER")
mysql_password = os.getenv("MYSQL_PASSWORD")
customers_table = os.getenv("CUSTOMERS_TABLE")
products_table = os.getenv("PRODUCTS_TABLE")

# Create Spark session
def create_spark_session(app_name: str = "CandyStoreAnalytics") -> SparkSession:
    """Create and configure Spark session with MongoDB and MySQL connectors"""
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,mysql:mysql-connector-java:8.0.33")
        .config("spark.mongodb.input.uri", mongodb_uri)
        .config("spark.mongodb.output.uri", f"{mongodb_uri}/{mongodb_db}")
        .getOrCreate()
    )

# Extract Data Function
def extract_data():
    """Extract data from MySQL and MongoDB into PySpark DataFrames"""
    try:
        logger.info("\nStarting Data Extraction Process")
        spark = create_spark_session()
        
        config = {
            'mysql_url': mysql_url,
            'mysql_user': mysql_user,
            'mysql_password': mysql_password,
            'mongodb_uri': mongodb_uri,
            'mongodb_db': mongodb_db,
            'customers_table': customers_table,
            'products_table': products_table,
        }
        date_range = get_date_range(os.getenv("MONGO_START_DATE"), os.getenv("MONGO_END_DATE"))
        
        # Load Data from MySQL and MongoDB
        customers_df, products_df, transactions_df = load_dataframes(spark, config, date_range)
        
        # Store the loaded data in XCom for downstream tasks
        return customers_df, products_df, transactions_df
    
    except Exception as e:
        logger.error(f"Error in Data Extraction: {e}")
        traceback.print_exc()

# Transform Data Function
def transform_data(**kwargs):
    """Process and transform data as per the business requirements"""
    try:
        logger.info("\nStarting Data Transformation Process")
        
        # Retrieve data from previous task
        customers_df = kwargs['ti'].xcom_pull(task_ids='extract_data')[0]
        products_df = kwargs['ti'].xcom_pull(task_ids='extract_data')[1]
        transactions_df = kwargs['ti'].xcom_pull(task_ids='extract_data')[2]
        
        # Process data using the DataProcessor class
        processor = DataProcessor()
        processor.products_df = products_df
        processor.transactions_df = transactions_df
        processor.run()
        
        logger.info("\n✅ Data Transformation Completed!")
        return processor
        
    except Exception as e:
        logger.error(f"Error in Data Transformation: {e}")
        traceback.print_exc()

# Load Data Function
def load_data(**kwargs):
    """Load processed data into the target system"""
    try:
        logger.info("\nStarting Data Load Process")
        
        processor = kwargs['ti'].xcom_pull(task_ids='transform_data')
        # Further processing steps like saving the transformed data into a database or file system
        
        logger.info("\n✅ Data Load Completed!")
        
    except Exception as e:
        logger.error(f"Error in Data Load: {e}")
        traceback.print_exc()

# Forecasting Data Function
def forecast_data():
    """Perform sales and profit forecasting"""
    try:
        logger.info("\nStarting Forecasting Process")
        
        # Run forecasting model (ProphetForecaster)
        forecaster = ProphetForecaster()
        forecaster.run(mysql_url, mysql_user, mysql_password)
        
        logger.info("\n✅ Forecasting Completed!")
    
    except Exception as e:
        logger.error(f"Error in Forecasting: {e}")
        traceback.print_exc()

# Helper function to get date range for MongoDB extraction
def get_date_range(start_date: str, end_date: str) -> list[str]:
    """Generate a list of dates between start and end date"""
    start = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")
    return [(start + timedelta(days=i)).strftime("%Y%m%d") for i in range((end - start).days + 1)]

# Airflow DAG definition
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 5),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "order_processing_dag",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
)

# Define tasks in the DAG
extract_data_task = PythonOperator(task_id="extract_data", python_callable=extract_data, dag=dag)
transform_data_task = PythonOperator(task_id="transform_data", python_callable=transform_data, provide_context=True, dag=dag)
load_data_task = PythonOperator(task_id="load_data", python_callable=load_data, provide_context=True, dag=dag)
forecast_data_task = PythonOperator(task_id="forecast_data", python_callable=forecast_data, dag=dag)

# Set task dependencies (execution order)
extract_data_task >> transform_data_task >> load_data_task >> forecast_data_task