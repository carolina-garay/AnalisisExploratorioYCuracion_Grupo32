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
        
    
    def remove_outliers(self, df, column_name):
    
        ''' Outliers are removed '''
    
        df = df.copy()
    
        q1 = df[column_name].quantile(0.25)
        q3 = df[column_name].quantile(0.75)
        iqr = q3 - q1 #Interquartile range
        fence_low  = q1 - 1.5 * iqr
        fence_high = q3 + 1.5 * iqr
        df = df.loc[(df[column_name] > fence_low) & (df[column_name] < fence_high)]
    
        df = df.reset_index(drop=True)

        return df        
        
    def merge_df(self):
    
        ''' The datasets are combined '''
        
        start_time = time.time()
    
        kaggle_new_df = self.kaggle_df

        kaggle_new_df_outlier = self.remove_outliers(kaggle_new_df, "Price")
        
        kaggle_new_df_outlier = kaggle_new_df_outlier[kaggle_new_df_outlier['Car'] < 7]
    
        airbnb_df_agg = self.airbnb_df.groupby("zipcode").agg(airbnb_record_count=("price", "count"),
            airbnb_daily_price_mean=("price", "mean"),
            airbnb_weekly_price_mean=("weekly_price", "mean"),
            airbnb_monthly_price_mean=("monthly_price", "mean"),
            airbnb_daily_price_median=("price", "median"),
            airbnb_weekly_price_median=("weekly_price", "median"),
            airbnb_monthly_price_median=("monthly_price", "median"))\
            .reset_index()
        
        
        airbnb_df_agg = airbnb_df_agg[airbnb_df_agg["airbnb_record_count"] > 2]
        
        airbnb_df_agg["zipcode"] = airbnb_df_agg["zipcode"].astype(float, errors = "raise")
        
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
        
        
        
        
        
        
   

