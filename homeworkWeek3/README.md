# WEEK 3 HOMEWORK

Data source:
**https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv**

## Q1 What is the count for fhv vehicle records for year 2019?

WHEN INGEST DATA INTO GCS BUCKET, WITHIN hw3_etl_web_to_gcs.py FILE THERE IS A COMMAND PRINT, IN LINE 75, THAT ALLOW US TO MEET THE TOTAL OF PROCESSED ROWS FOR 2019 IN FHV TRIP DATA:

**The total of processed rows are: 43244696**



## Q2 What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

CREATING AN EXTERNAL TABLE:

**CREATE OR REPLACE EXTERNAL TABLE `dtc-de-course-375103.deZoomcampDatasetBQ.external_fhv_tripdata_2019`**
**OPTIONS (format = 'CSV', uris = ['gs://prefect-de-zoomcamp-jfcohdz/data/fhv_tripdata_2019-*.csv.gz']);**

CREATING BQ TABLE:

**CREATE OR REPLACE TABLE `dtc-de-course-375103.deZoomcampDatasetBQ.bq_fhv_tripdata_2019`**
**AS SELECT * FROM `dtc-de-course-375103.deZoomcampDatasetBQ.external_fhv_tripdata_2019`;**

HIGHLIGHTING THE QUERY ON EXTERNAL TABLE:

**SELECT count(distinct Affiliated_base_number) FROM `dtc-de-course-375103.deZoomcampDatasetBQ.external_fhv_tripdata_2019`;**

AND HIGHLIGHTING THE QUERY ON BQ TABLE:

**SELECT count(distinct Affiliated_base_number) FROM `dtc-de-course-375103.deZoomcampDatasetBQ.bq_fhv_tripdata_2019`;**

SO, THE ANSWER FOR THIS QUESTION IS:

**0 MB for the External Table and 317.94MB for the BQ Table**



## Q3 How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?

THE QUERY FOR THIS QUESTION:

**select count(*)**
**from `dtc-de-course-375103.deZoomcampDatasetBQ.external_fhv_tripdata_2019`**
**where PUlocationID is null and DOlocationID is null;**

SO, THE ANSWER FOR THIS QUESTION IS:

**717,748**



## Q4 What is the best strategy to optimize the table if query always filter by pickup_datetime and order by affiliated_base_number?

AS THE DE ZOOMCAMP 3.1.1 - DATA WAREHOUSE AND BIGQUERY VIDEO SHOWS, PARTITIONING LET US FILTER DATA BY DATE OR OTHER FIELDS, WHILE CLUSTERING LET US ORDER A FIELD  IN A ALPHABETICAL WAY, SO THE FIRST THING IS TO FILTER THEN ORDER THAT FILTERING:

**Partition by pickup_datetime Cluster on affiliated_base_number**




## Q5 Implement the optimized solution you choose for Q4. Write a query to retrieve the distinct affiliated_base_number between pickup_datetime 2019/03/01 and 2019/03/31 (inclusive). Use the BQ table and the table in the from clause to the partitioned table you created for Q4 and note the estimated bytes processed. What are these values?

CREATING THE TABLE SUGGESTED IN Q4:

**CREATE OR REPLACE TABLE `dtc-de-course-375103.deZoomcampDatasetBQ.BQ_fhv_tripdata_2019_partitoned_clustered`**
**PARTITION BY DATE(pickup_datetime)**
**CLUSTER BY Affiliated_base_number AS**
**SELECT * FROM `dtc-de-course-375103.deZoomcampDatasetBQ.bq_fhv_tripdata_2019`;**

PERFORMING THE REQUESTED QUERY ON BQ TABLE:

**select distinct(Affiliated_base_number)**
**from `dtc-de-course-375103.deZoomcampDatasetBQ.bq_fhv_tripdata_2019`**
**WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-04-01';**

THEN PEFORMING THE SAME QUERY BUT ON THE PARTITONED AND CLUSTERED TABLE FROM Q4:

**select distinct(Affiliated_base_number)** 
**from `dtc-de-course-375103.deZoomcampDatasetBQ.BQ_fhv_tripdata_2019_partitoned_clustered`**
**WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-04-01';**

THE QUERY ON BQ TABLE EFFECTIVELY THROWS US 647.87 MB BUT THE QUERY ON PARTITONED TABLE GIVE US 24.1 MB, SO THE CLOSER ANSWER IS:

**647.87 MB for non-partitioned table and 23.06 MB for the partitioned table**


## Q6 Where is the data stored in the External Table you created?

**""External tables are similar to standard BigQuery tables, in that these tables store their metadata and schema in BigQuery storage. However, their data resides in an external source. External tables are contained inside a dataset..."**

AS WE CAN LISTEN IN MINUTE 11:15, CREATE AN EXTERNAL TABLE SECTION, FROM DE ZOOMCAMP 3.1.1 DATA WAREHOUSE AND BIGQUERY:

**""...because the data itself is not inside bigquery, it's in an external system such as google cloud storage..""**

THEN THE MOST APPROXIMATE ANSWER WOULD BE:

**google cloud storage or GCP Bucket**



## Q7 It is best practice in Big Query to always cluster your data:

**False**

BECAUSE DEPENDS ON THE NEED OF THE QUERY, EVEN IF CLUSTERING DOES IMPROVEMENTS ON QUERY PERFORMANCE, NOT ALWAYS WANT TO DO GROUPING IN ORDER.



## Q8 (Not required) Create a data pipeline to download the gzip files and convert them into parquet. Upload the files to your GCP Bucket and create an External and BQ Table.

TOTAL OF PROCESSED ROWS:

**The total of processed rows from web to local storing are: 43244696 rows**

CREATING EXTERNAL TABLE WITH PARQUET FORMAT FILES:

**CREATE OR REPLACE EXTERNAL TABLE `dtc-de-course-375103.deZoomcampDatasetBQ.external_fhv_tripdata_2019_parquet`**
**OPTIONS (format = 'PARQUET',uris = ['gs://prefect-de-zoomcamp-jfcohdz/data/fhv_tripdata_2019-*.parquet']);**

CHECKING FOR ANY KIND OF TROUBLE IF THEY WOULD EXIST:

**select * from `dtc-de-course-375103.deZoomcampDatasetBQ.external_fhv_tripdata_2019_parquet` limit 3;**

THERES ARE PROBLEMS WITH DATA TYPES IN DOlocationID AND PUlocationID, IT WILL BE NECESSARY TO MODIFY THE PIPELINE, CHANGE DATA TYPES IN BOTH COLUMNS, LIKE IT SHOWN IN hw3_etl_web_to_gcs_parquet.py FILE. THEN PERFORM A NEW CREATION OF EXTERNAL TABLE, I DELETE THE PREVIOUS EXTERNAL TABLE AND ALSO CREATE A BQ TABLE:

**CREATE OR REPLACE TABLE `dtc-de-course-375103.deZoomcampDatasetBQ.bq_fhv_tripdata_2019_parquet`**
**AS SELECT * FROM `dtc-de-course-375103.deZoomcampDatasetBQ.external_fhv_tripdata_2019_parquet`;**

THEN PERFORM THE QUERIES:

**SELECT count(distinct Affiliated_base_number) FROM `dtc-de-course-375103.deZoomcampDatasetBQ.external_fhv_tripdata_2019_parquet`;**

**SELECT count(distinct Affiliated_base_number) FROM `dtc-de-course-375103.deZoomcampDatasetBQ.bq_fhv_tripdata_2019_parquet`;**

THIS GIVE US:

**0 MB for the External Table and 317.94MB for the BQ Table**

CREATING THE TABLE SUGGESTED IN Q4:

**CREATE OR REPLACE TABLE `dtc-de-course-375103.deZoomcampDatasetBQ.BQ_fhv_tripdata_2019_partitoned_clustered_parquet`**
**PARTITION BY DATE(pickup_datetime)**
**CLUSTER BY Affiliated_base_number AS**
**SELECT * FROM `dtc-de-course-375103.deZoomcampDatasetBQ.bq_fhv_tripdata_2019`;**

PERFORMING THE REQUESTED QUERY ON BQ TABLE:

**select distinct(Affiliated_base_number)**
**from `dtc-de-course-375103.deZoomcampDatasetBQ.bq_fhv_tripdata_2019`**
**WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-04-01';**

THEN PEFORMING THE SAME QUERY BUT ON THE PARTITONED AND CLUSTERED TABLE FROM Q4:

**select distinct(Affiliated_base_number)** 
**from `dtc-de-course-375103.deZoomcampDatasetBQ.BQ_fhv_tripdata_2019_partitoned_clustered_parquet`**
**WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-04-01';**

THE QUERY ON BQ TABLE AGAIN THROWS US 647.87 MB AND THE QUERY ON PARTITONED TABLE GIVE US 24.1 MB, THE SAME ANSWER AS WITH CSV FILES:

**647.87 MB for non-partitioned table and 23.06 MB for the partitioned table**













































