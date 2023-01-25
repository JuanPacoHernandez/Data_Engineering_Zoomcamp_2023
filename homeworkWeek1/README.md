# WEEK 1 HOMEWORK

## Q1

**docker --help build**

## Q2

CREATE A DOCKER FILE:

**FROM python:3.9**

**WORKDIR /app**

**ENTRYPOINT ["/bin/bash"]**

THEN BUILD DOCKER IN TERMINAL, WHERE DOCKER FILE IS:

**docker build -t test:python .**

NEXT, RUN DOCKER:

**docker run -it test:python**

WHITIN INTERACTIVE BASH, TYPE:

**pip list**

## Q3

**select count(*) from green_taxi_data WHERE cast(lpep_pickup_datetime as date) = '2019-01-15' and cast(lpep_dropoff_datetime as date) = '2019-01-15';**

## Q4

**select cast(lpep_pickup_datetime as date) from green_taxi_data where trip_distance = (select max(trip_distance) from green_taxi_data);**

## Q5

**select count(*) from green_taxi_data WHERE cast(lpep_pickup_datetime as date) = '2019-01-01' and passenger_count = 2;**
**select count(*) from green_taxi_data WHERE cast(lpep_pickup_datetime as date) = '2019-01-01' and passenger_count = 3;**

## Q6

with cte as (select "DOLocationID", tip_amount 
             from green_taxi_data 
             where "PULocationID" = (select "LocationID" 
                                     from zones 
                                     where "Zone" = 'Astoria')
             order by tip_amount desc 
             limit 1)

select "Zone" from zones where "LocationID" = (select "DOLocationID" 
                                               from cte);






