# Import the os Module
import os 

# Import the decouple library
from decouple import config

# All methods and functions from the etl.py file are imported
from etl import *

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
    
    '''
        I call the class Pipeline
    '''
    Pipeline(PATH_KAGGLE, PATH_AIRBNB, kaggle_features, interesting_features, DB_ENGINE, DB_NAME).create_database()   
