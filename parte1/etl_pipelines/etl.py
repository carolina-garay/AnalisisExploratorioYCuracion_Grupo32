import pandas as pd

import os

import time

from sqlalchemy import create_engine, text


class Pipeline():

    def __init__(self, kaggle_path, airbnb_path, kaggle_features, interesting_features, db_engine, db_name):
        
        ''' Class constructor is declared '''
    
        start_time = time.time()
    
        self.kaggle_df = pd.read_csv(kaggle_path, usecols=kaggle_features)
        
        self.airbnb_df = pd.read_csv(airbnb_path, usecols=interesting_features)
        
        self.db_engine = db_engine
        
        self.db_name = db_name
        
        print("La etapa de extraccion tardo: --- %s segundos ---" % (time.time() - start_time)) 
        
    def merge_df(self):
    
        ''' The datasets are combined '''
        
        start_time = time.time()
    
        kaggle_new_df = self.kaggle_df
        
        kaggle_new_df_outlier = kaggle_new_df[(kaggle_new_df["Price"] > 200000) & (kaggle_new_df["Price"] < 3000000)]
        
        kaggle_new_df_outlier = kaggle_new_df_outlier[kaggle_new_df_outlier['Car'] < 7]
        
        kaggle_new_df_outlier =  kaggle_new_df_outlier[kaggle_new_df_outlier['YearBuilt'] != 1196]
        
        high_extreme_values = [44515, 6791, 3558, 3112, 1561, 1143, 1041, 1022, 934, 808, 792]
        low_extreme_values = [i for i in range(10)]
        outliers= high_extreme_values + low_extreme_values
        
        kaggle_new_df_outlier = kaggle_new_df_outlier[~kaggle_new_df_outlier["BuildingArea"].isin(outliers)] # El símbolo ~ me brinda la selección opuesta al isin
        
        self.airbnb_df["zipcode"] = pd.to_numeric(self.airbnb_df.zipcode, errors="coerce")
        
        airbnb_df_agg = self.airbnb_df.groupby("zipcode").agg(airbnb_record_count=("price", "count"),
            airbnb_daily_price_mean=("price", "mean"),
            airbnb_weekly_price_mean=("weekly_price", "mean"),
            airbnb_monthly_price_mean=("monthly_price", "mean"),
            airbnb_daily_price_median=("price", "median"),
            airbnb_weekly_price_median=("weekly_price", "median"),
            airbnb_monthly_price_median=("monthly_price", "median"))\
            .reset_index()
        
        
        airbnb_df_agg = airbnb_df_agg[airbnb_df_agg["airbnb_record_count"] > 2]
        
        merge_df = kaggle_new_df_outlier.merge(airbnb_df_agg, how="left", left_on="Postcode", right_on="zipcode")
        
        merge_df.to_csv(os.path.join(os.getcwd(), "merge_df.csv"), index=False)
        
        print("La etapa de transformacion tardo: --- %s segundos ---" % (time.time() - start_time))
        
        return merge_df
    
    
    def create_database(self):
    
        ''' The dataset is loaded into the database '''
    
        start_time = time.time()
    
        engine = create_engine(
            r"{}:///{}".format(
                self.db_engine,
                os.path.join(
                    os.getcwd(),
                    self.db_name,
                )
            ), 
            echo=True
        )
        
        merge_df = self.merge_df()
        
        merge_df.to_sql("melbourne", con=engine, if_exists="replace", index=False)
        
        print("La etapa de carga tardo: --- %s segundos ---" % (time.time() - start_time))
        
        
        
        
        
        
   

