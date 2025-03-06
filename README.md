Project 2: Batch Processing ETL Pipeline for Candy Store

Overview

Tiger’s Candy, a popular candy store originating from the RIT campus, has seen substantial growth. To manage its expanding operations, the store requires an automated system that processes online orders in batches.

This project implements a batch processing ETL (Extract, Transform, Load) pipeline to handle raw order transactions on a daily basis. The pipeline performs essential tasks such as validating transaction details, checking inventory levels, and forecasting sales and profits.

Dataset Overview

The dataset includes various information related to customers, products, and order transactions.

Customers Dataset
	•	customer_id: Unique identifier for each customer
	•	first_name: First name of the customer
	•	last_name: Last name of the customer
	•	email: Customer’s email address
	•	address: Physical address of the customer
	•	phone: Phone number of the customer

Products Dataset
	•	product_id: Unique identifier for each product
	•	product_name: Name of the product
	•	product_category: Category of candy (e.g., Chocolate, Gummy)
	•	sales_price: Retail price of the product
	•	cost_to_make: Manufacturing cost of the product
	•	stock: Inventory level of the product

Order Transactions (Raw JSON Format)

Each transaction file consists of:

{
  "transaction_id": 73434473,
  "customer_id": 29,
  "timestamp": "2024-02-02T12:00:40.808092",
  "items": [
    {"product_id": 17, "product_name": "Candy A", "qty": 5},
    {"product_id": 3, "product_name": "Candy B", "qty": 2}
  ]
}

Required Software Packages

To set up your environment for the project, the following packages must be installed:

pip install apache-airflow
pip install pyspark
pip install python-dotenv
pip install prophet

Setup Instructions and Expected Outputs

Setting up the Virtual Environment
	1.	Create a virtual environment in your project directory:
    python3 -m venv venv

    2.	Activate the virtual environment:
    source venv/bin/activate

    Running the Code
	3.	After activating the virtual environment, run the main script from the project-2 directory:
    python3 src/main.py

Airflow DAG Setup

 Install Apache Airflow in your virtual environment:
 pip install apache-airflow

Initialize the Airflow database:
airflow db init

 Create an admin user account and set the password:
 airflow users create --username admin --firstname yourName --lastname lastName --role Admin --email email@email.com

 4. Start the Airflow webserver and scheduler in two different terminals:
 	•	Terminal 1:
    airflow webserver --port 8080

    •	Terminal 2:
    airflow scheduler

5. Copy the DAG script (order_processing_dag.py) to the ~/airflow/dags directory.

6. Access Airflow UI:
	•	Open a web browser and visit http://localhost:8080.
	•	Log in with your credentials.
	•	Locate the order_processing_dag in the DAGs list and toggle it to make it active.
	•	Open the order_processing_dag and click the “Play” button at the top right to run it.

