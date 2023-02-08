from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(log_prints=True, retries=3)
# -> MEANS THAT THIS FUNCTION RETURNS A PATH
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    # DOWNLOAD TRIP DATA FROM GCS
    # PATH EXTRACTED FROM BUCKET
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("de-zoomcamp-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"../data/{gcs_path}")
    
@task(log_prints=True)
# -> MEANS THAT THIS FUNCTION RETURNS A DATAFRAME
def transform(path: Path) -> pd.DataFrame:
    # DATA CLEANING FILLING "NA" WITH "0"
    df = pd.read_parquet(path)
    df = df.rename(columns={'tpep_pickup_datetime':'lpep_pickup_datetime', 'tpep_dropoff_datetime':'lpep_dropoff_datetime'})
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    #df['passenger_count'] = df['passenger_count'].fillna(0)
    print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df


@task(log_prints=True, retries=3)
# -> MEANS THAT THIS FUNCTION RETURNS A DATAFRAME
def write_bq(df: pd.DataFrame) -> None:
    # WRITE DATAFRAME TO BIGQUERY
    
    # ADDING CREDENTIALS
    gcp_credentials_block = GcpCredentials.load("de-zoomcamp-gcs-credentials")
    
    # SAVING INTO THE DATA WAREHOUSE
    df.to_gbq(destination_table = 'deZoomcampDatasetBQ.greenTaxi', 
              project_id = 'dtc-de-course-375103',
              credentials = gcp_credentials_block.get_credentials_from_service_account(),
              # 500,00 ROWS BY TIME
              chunksize = 500_00,
              if_exists = 'append')  
    
@flow(log_prints=True)
def el_gcs_to_bq(color: str, year: int, month: int) -> int:
    # MAIN ETL FLOW TO LOAD DATA INTO BIG QUERY
    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df)
    
    # NOTICE WHEN INGESTION FINISH
    print(f"\n\nData ingestion for {color} taxi, year {year} and month {month} from Bucket into the Big Query database has finished \n\n")
    
    return len(df)
    

@flow(log_prints=True)
def el_ParentFlow(months: list[int] = [2,3], year: int = 2019, color: str = 'yellow'):
    rows = 0
    for month in months:
        rows =+ el_gcs_to_bq(color, year, month)
        
    
    
if __name__ == '__main__':
    color = 'yellow'
    year = 2019
    months = [2,3]
    el_ParentFlow()




