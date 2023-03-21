# WEEK 6 HOMEWORK

## Q1 Please select the statements that are correct 

- Kafka Node is responsible to store topics
- Retention configuration ensures the messages not get lost over specific period of time

## Q2 Please select the Kafka concepts that support reliability and availability

- Topic Replication

## Q3 Please select the Kafka concepts that support scaling 

- Topic Partitioning

## Q4 Please select the attributes that are good candidates for partitioning key. Consider cardinality of the field you have selected and scaling aspects of your application

- vendor_id

## Q5 Which configurations below should be provided for Kafka Consumer but not needed for Kafka Producer *

- Deserializer Configuration
- Topics Subscription
- Group-Id
- Offset

## Please implement a streaming application, for finding out popularity of PUlocationID across green and fhv trip datasets. Please use the datasets fhv_tripdata_2019-01.csv.gz and green_tripdata_2019-01.csv.gz

Run producer, data dir must contains both CSV files:

**python3 producer_taxi.py --input_green=/data/green_tripdata_2019-01.csv.gz --input_fhv=/data/fhv_tripdata_2019-01.csv.gz**

Then consumer:

**python3 consumer.py**








