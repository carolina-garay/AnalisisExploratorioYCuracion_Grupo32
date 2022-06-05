# Import the os Module
import os 

# Import the decouple library
from decouple import config
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
# All methods and functions from the etl.py file are imported
from etl import *

default_args = {
	"owner": "Pablo Nicolas Ramos",
	"depends_on_past": False,
	"email": ["pablonicolasr777@gmail.com"],
	"email_on_failure": False,
	"email_on_retry": False,
	"retries": 5,
	"retry_delay": timedelta(minutes=1)
}

# Define the dag, the start date and how frequently it runs
# I choose the dag to run everyday by using 1440 minutes

dag = DAG(
	dag_id = "diploDAG",
	default_args = default_args,
	start_date = datetime(2022, 6, 3),
	schedule_interval = timedelta(minutes=1440)
)

# Environment variables are imported
DB_ENGINE = config("DB_ENGINE")
DB_NAME = config("DB_NAME")

# The urls that provide the data are declared
PATH_KAGGLE = "https://cs.famaf.unc.edu.ar/~mteruel/datasets/diplodatos/melb_data.csv"
PATH_AIRBNB = "https://cs.famaf.unc.edu.ar/~mteruel/datasets/diplodatos/cleansed_listings_dec18.csv"

#
kaggle_features = ["YearBuilt","BuildingArea", "Car", "Postcode", "Rooms", "Price", "Regionname", "Suburb", "Type"]
interesting_features = ["zipcode", "price", "weekly_price", "monthly_price"]



if __name__ == "__main__":

	

	task1 = PythonOperator(
		task_id = "get_data",
		provide_context=True,
		python_callable = Pipeline(PATH_KAGGLE, PATH_AIRBNB, kaggle_features, interesting_features, DB_ENGINE, DB_NAME).create_database,
		dag=dag
	)

	task1
		  
