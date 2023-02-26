# WEEK 4 HOMEWORK

## Q1 What is the count of records in the model fact_trips after running all models with the test run variable disabled and filtering for 2019 and 2020 data only (pickup datetime)?

Querying with:

**SELECT count(*) FROM `dtc-de-course-375103.dbt_AnalyticsProject.fact_trips`;**

Throws us 76,595,000 records so the closest answer would be 71648442

## Q2 What is the distribution between service type filtering by years 2019 and 2020 data as done in the videos?

Doing the analyisis, having a 76,595,000 of total records, then yellow cabs will produce 63,610,125 records then the ratio is 0.83 and the 12,984,875 records for the green cabs will produce 0.17 of ratio. Querying the table:

**SELECT sum(case when 'service_type' = 'Green' then 1 else 0 end)/count(*) as green_ratio, sum(case when `service_type` = 'Yellow' then 1 else 0 end)/count(*) as yellow_ratio FROM `dtc-de-course-375103.dbt_AnalyticsProject.fact_trips`;**

Then chossing the closest answer:

**89.9/10.1**


## Q3 What is the count of records in the model stg_fhv_tripdata after running all models with the test run variable disabled (:false)?

The next query throws us the answer:

**SELECT count(*)  FROM `dtc-de-course-375103.deZoomcampDatasetBQ.stg_fhv_tripdata`**

This gives us 43,244,696


# Q4 What is the count of records in the model fact_fhv_trips after running all dependencies with the test run variable disabled (:false)?

The next query throws us the answer:

**SELECT count(*) FROM `dtc-de-course-375103.dbt_analytics.fact_fhv_trips` where pickup_datetime is not null and dropoff_datetime is not null;**

This gives us 43,244,696, the closest answer is 42,998,722


# Q5 What is the month with the biggest amount of rides after building a tile for the fact_fhv_trips table?

the next wury solve the question:

**SELECT extract(MONTH from pickup_datetime) as month,count(tripid) as counter FROM `dtc-de-course-375103.dbt_analytics.fact_fhv_trips` group by month order by counter desc;**

The answer is January with 23,143,222 records


















