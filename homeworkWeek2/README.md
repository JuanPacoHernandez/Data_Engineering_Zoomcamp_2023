# WEEK 2 HOMEWORK

## Q1 Load January 2020 data

RUNNING THE etl_web_to_gcs.py FILE:

**python3 etl_web_to_gcs.py**

THIS CODE LINE GIVE THE ANSWER:

**print(f"ROWS: {len(df)}")**

THE ANSWER:    **447,770**



## Q2 Scheduling with Cron

TO CREATE THE DEPLOYMENT:

**prefect deployment build etl_web_to_gcs.py:etl-gcs-flow -n "Web to GCS-Bucket ETL" --cron "0 5 1 * *" -a**



## Q3 Loading data to BigQuery

CREATE THE DEPLOYMENT BUILD TO THE el_gcs_to_bq.py FILE WITH PARAMETERIZED FLOW:

**prefect deployment build el_gcs_to_bq.py:el_ParentFlow -n "Parameterized EL"**

NEXT APPLY THE DEPLOYMENT:

**prefect deployment apply el_ParentFlow-deployment.yaml**

NEXT START THE AGENT, THIS CODE IS DISPLAYED IN WORK QUEUES AT UI DASHBOARD:

**prefect agent start -q default**

NOW YOU CAN RUN LOCALLY THE FILE:

**pythone3 el_gcs_to_bq.py**

THIS GIVES US **14,851,920** OF PROCESSED ROWS



# Q4 Github Storage Block

CREATE THE DEPLOYMENT BUILD TO THE el_gcs_to_bq.py FILE WITH PARAMETERIZED FLOW:

**prefect deployment build Q4_web_to_gcs.py:main_flow --name github_storage_deploy --tag flow-storage-block -sb github/flow-storage-block -a**

NEXT APPLY THE DEPLOYMENT:

**prefect deployment apply main_flow-deployment.yaml**

NEXT START THE AGENT, THIS CODE IS DISPLAYED IN WORK QUEUES AT UI DASHBOARD:

**prefect agent start -q default**

NOW YOU CAN RUN LOCALLY THE FILE:

**python3 Q4_web_to_gcs.py**

THIS GIVES US **88605** OF PROCESSED ROWS



# Q5 Email or Slack notifications

USING THE SAME DEPLOYMENT FROM Q4 BUT SETTING NEW PARAMETERS FOR green taxi, 2019, april IN THE MAIN FLOW GIVES US 

**SORRY, I COULDN'T COMPLETE THIS QUESTION I PUT ANY ANSWER NOT THE RIGHT ONE**




# Q6 Secrets

THE UI DASHBOARD CREATE A 8 ASTERISKS CHARACTERS AFTER SECRET BLOCK CREATION




