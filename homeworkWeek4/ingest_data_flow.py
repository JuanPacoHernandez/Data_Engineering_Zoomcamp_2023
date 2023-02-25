#!/usr/bin/env python
# coding: utf-8

# IMPORTING LIBRARIES
import argparse
import pandas as pd
from sqlalchemy import create_engine
import os
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_sqlalchemy import SqlAlchemyConnector
from pathlib import Path



@task(log_prints=True, retries=2)
def extract_data(url: str) -> pd.DataFrame:
    # READING CSV FILE TO DATAFRAME
    df = pd.read_parquet(url)
    return df



@task(log_prints=True)
def transform_data(df: pd.DataFrame, tipo: str) -> pd.DataFrame:
    # CONVERT TO DATETIME FORMAT TWO COLUMNS
    if tipo == 'green':
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    elif tipo == 'fhv':
        df.pickup_datetime = pd.to_datetime(df.pickup_datetime)
        df.dropOff_datetime = pd.to_datetime(df.dropOff_datetime)
        df['PUlocationID'] = df['PUlocationID'].astype('Int64')
        df['DOlocationID'] = df['DOlocationID'].astype('Int64')
    else:
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    return df
    
    
@task(log_prints=True)
def write_local(df: pd.DataFrame, year: int, month: int, tipo: str) -> Path:
    # WRITE DF OUT LOCALLY AS CSV FILE
    dataset_path = f"{tipo}_tripdata_{year}-{month:02}"
    path = Path(f"data/{tipo}/{dataset_path}.parquet")
    if not path.parent.is_dir():
        path.parent.mkdir(parents=True)
    df.to_parquet(path, compression="gzip")
    return path
    
    
    
@task(log_prints=True,retries=2)
def load_data(df: pd.DataFrame, tipo: str) -> int:
    user = 'postgres'
    password = 'root'
    host = 'localhost'
    port = '5432'
    db = "nyc_taxi"
    table_name = f"{tipo}_taxi_data"
    
    # IF NOT USING BLOCKS USE:
    # CONNECTION WITH POSTGRESQL
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()
    
    # INGESTING DATA INTO POSTGRESQL
    # ~ df = pd.read_csv(path, encoding="utf-8")
    df.to_sql(name=table_name, con=engine, index=False, chunksize = 500_00, if_exists='append')
    return len(df)
    
    
    
@flow(log_prints=True)
def main_flow():
    tipo = 'yellow'
    year = 2019
    total_rows = 0
    for month in range(1,13):
        # IF NOT USING BLOCKS USE:
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{tipo}_tripdata_{year}-{month:02}.parquet"
        # ~ url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{tipo}/{tipo}_tripdata_{year}-{month:02}.csv.gz"
        # EXTRACTING DATA
        raw_data = extract_data(url)
        
        # TRANSFORMING DATA
        data_clean = transform_data(raw_data, tipo)
        
        # WRITE LOCALLY
        # ~ path = write_local(data_clean, year, month, tipo)
        
        # LOADING DATA
        rows = load_data(data_clean, tipo)
        
        total_rows += rows
    
    # NOTICE WHEN INGESTION FINISH
    print("\n\nData ingestion into the postgres database has finished \n\n")
    print(f"\n\nThe total of processed rows are {total_rows}\n\n")
    
if __name__ == '__main__':
    main_flow()
