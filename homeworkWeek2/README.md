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

CREATE AN AUTOMATION IN PREFECT CLOUD:

- Automations
- ADD +
- Trigger Type: Flow run state
- NEXT
- Action Type: Send a notification
- Block: Add Email block:
                     - Block name: email-notification-block
                     - Email: Add the email destination address
                     - CREATE

THEN MAKE SURE YOUR ARE LOGIN INTO PREFECT CLOUD

**prefect cloud login**

RUN:

**python3 Q5_web_to_gcs.py**

THIS SHOWS **514392** ROWS.



# Q6 Secrets

IN THE UI DASHBOARD CREATE A SECRET BLOCK, INTRODUCE A PASSWORD, THEN CREATE. AND THE RECENTLY BLOCK WILL THROW 8 ASTERISKS CHARACTERS




