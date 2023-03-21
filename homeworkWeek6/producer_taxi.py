#!/usr/bin/env python
# coding: utf-8
from confluent_kafka import Producer
from config import config
import pandas as pd
import argparse
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def callback(err, event):
    if err:
        print(f'Produce to topic {event.topic()} failed for event: {event.key()}')
    else:
        val = event.value().decode('utf8')
        print(f'{val} and sent to partition {event.partition()}.')
        
def ETL():
    # MAKING CONFIGURABLE WITH ARGUMENTS ON CALLING THE SCRIPT:
    parser = argparse.ArgumentParser()

    parser.add_argument('--input_green', required=True)
    parser.add_argument('--input_fhv', required=True)

    args = parser.parse_args()

    input_green = args.input_green
    input_fhv = args.input_fhv


    spark = SparkSession.builder \
        .appName('test') \
        .getOrCreate()
        
    df_green = spark.read.option("header", True).csv(input_green)

    df_green = df_green \
        .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
        .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')

    df_fhv = spark.read.option("header", True).csv(input_fhv)


    df_fhv = df_fhv \
        .withColumnRenamed('dropOff_datetime', 'dropoff_datetime')


    green_columns = [
        'PULocationID'
    ]

    fhv_columns = [
        'PULocationID'
        ]

    df_green_sel = df_green \
        .select(green_columns)

    df_fhv_sel = df_fhv \
        .select(fhv_columns)


    df_trips_data = df_green_sel.union(df_fhv_sel)

    df_trips_data.groupBy('PUlocationID') \
            .count() \
            .sort(col("count").desc()) \
            .show()

    print("<------------------  Process finished!  --------------------------->")
   
def taxi_producer(producer, key, year, month):
    value = f'{key} taxi CSV data for {year} month {month:02}'
    producer.produce(f'{key}_csv', value, key, on_delivery=callback)
    
if __name__ == '__main__':
    year = 2019
    month = 1
    ETL()
    producer = Producer(config)
    keys = ['fhv', 'green']
    [taxi_producer(producer, key, year, month) for key in keys]
    producer.flush()
    

    
    
    
 
