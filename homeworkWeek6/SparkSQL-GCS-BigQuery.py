#!/usr/bin/env python
# coding: utf-8

import argparse
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# MAKING CONFIGURABLE WITH ARGUMENTS ON CALLING THE SCRIPT:
parser = argparse.ArgumentParser()

parser.add_argument('--input_green', required=True)
parser.add_argument('--input_fhv', required=True)
parser.add_argument('--output', required=True)

args = parser.parse_args()

input_green = args.input_green
input_yellow = args.input_yellow
output = args.output


spark = SparkSession.builder \
    .appName('test') \
    .getOrCreate()
    
# Use the Cloud Storage bucket for temporary BigQuery export data used
# by the connector.
bucket = "dataproc-temp-us-central1-752054861720-sungup2j"
spark.conf.set('temporaryGcsBucket', bucket)


df_green = spark.read.parquet(input_green)

df_green = df_green \
    .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')

df_fhv = spark.read.parquet(input_yellow)


df_fhv = df_yellow \
    .withColumnRenamed('dropOff_datetime', 'dropoff_datetime')


green_colums = [
    'VendorID',
    'pickup_datetime',
    'dropoff_datetime',
    'store_and_fwd_flag',
    'RatecodeID',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'fare_amount',
    'extra',
    'mta_tax',
    'tip_amount',
    'tolls_amount',
    'ehail_fee',
    'improvement_surcharge',
    'total_amount',
    'payment_type',
    'trip_type',
    'congestion_surcharge'
]

fhv_columns = [
    'dispatching_base_num',
    'pickup_datetime',
    'dropoff_datetime',
    'PULocationID',
    'DOLocationID',
    'SR_Flag',
    'Affiliated_base_number'
    ]



df_green_sel = df_green \
    .select(green_colums) \
    .withColumn('service_type', F.lit('green'))

df_fhv_sel = df_fhv \
    .select(fhv_colums) \
    .withColumn('service_type', F.lit('fhv'))


df_trips_data = df_green_sel.union(df_fhv_sel)

df_trips_data.createOrReplaceTempView('trips_data')


df_result = spark.sql("""
SELECT 
    PULocationID AS revenue_zone,
    count(1) as Number_of_PULocationID
FROM
    trips_data
GROUP BY
    Number_of_PULocationID
""")


# Saving the data to BigQuery
df_result.write.format('bigquery') \
  .option('table', output) \
  .save()


print("<------------------  Process finished!  --------------------------->")




