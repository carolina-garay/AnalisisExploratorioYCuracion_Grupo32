import os

from decouple import config
from etl import *


DB_ENGINE = config("DB_ENGINE")
DB_NAME = config("DB_NAME")
PATH_KAGGLE = "https://cs.famaf.unc.edu.ar/~mteruel/datasets/diplodatos/melb_data.csv"
PATH_AIRBNB = "https://cs.famaf.unc.edu.ar/~mteruel/datasets/diplodatos/cleansed_listings_dec18.csv"

kaggle_features = ["YearBuilt","BuildingArea", "Car", "Postcode", "Rooms", "Price"]
interesting_features = ['zipcode','price', 'weekly_price', 'monthly_price']


if __name__ == "__main__":
    
    Pipeline(PATH_KAGGLE, PATH_AIRBNB, kaggle_features, interesting_features, DB_ENGINE, DB_NAME).create_database()   
