# WEEK 1 HOMEWORK CODE PART A

## Q1 Knowing docker tags

THIS COMMAND WILL DISPLAY THE TAGS, THEN I SEARCH FOR THE TAG WHICH HAS THE FOLLOWING TEXT: *Write the image ID to the file*

**docker --help build**

## Q2 Understanding docker first run

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

## Q3 Count records

select count(*) 
from green_taxi_data 
where cast(lpep_pickup_datetime as date) = '2019-01-15' 
and cast(lpep_dropoff_datetime as date) = '2019-01-15';

## Q4 Largest trip for each day

select cast(lpep_pickup_datetime as date) 
from green_taxi_data 
where trip_distance = (select max(trip_distance) 
                       from green_taxi_data);

## Q5 The number of passengers

select count(*) 
from green_taxi_data 
where cast(lpep_pickup_datetime as date) = '2019-01-01' 
and passenger_count = 2;

select count(*) 
from green_taxi_data 
where cast(lpep_pickup_datetime as date) = '2019-01-01' 
and passenger_count = 3;

## Q6 Largest tip

with cte as (select "DOLocationID", tip_amount 
             from green_taxi_data 
             where "PULocationID" = (select "LocationID" 
                                     from zones 
                                     where "Zone" = 'Astoria')
             order by tip_amount desc 
             limit 1)

select "Zone" from zones where "LocationID" = (select "DOLocationID" 
                                               from cte);
                                               
                                               
# WEEK 1 HOMEWORK CODE PART B

## Q1 Creating Resources

AFTER MODIFIYING THE main.tf AND variables.tf FILES, TYPE IN TERMINAL:

**terraform init**

**terraform plan**    (INTRODUCE PROJECT ID)

**terraform apply**   (INTRODUCE PROJECT ID, THEN 'yes')

**terraform destroy** (TO ENSURE KILL THE RESOURCE)






