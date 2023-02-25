from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

@task(log_prints=True, retries=3) 
# -> MEANS THAT THIS FUNCTION RETURNS A DATAFRAME
#  FETCH IS EXPECTING A DATASET_URL VARIABLE OF STRING TYPE
def fetch(dataset_url: str) -> pd.DataFrame:
    # READ DATA FROM WEB 
    df = pd.read_csv(dataset_url)
    return df 
    
@task(log_prints=True)
def clean(df: pd.DataFrame):
    #  FIXING DTYPE DATETIME ISSUES
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df
    

@task(log_prints=True, retries=3)
# -> MEANS THAT THIS FUNCTION RETURNS A PATH
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    # WRITE DF OUT LOCALLY AS PARQUET FILE
    path = Path(f"/home/m710/Escritorio/Data_Engineering_Zoomcamp/week2_Workflow_Orchestration/data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path
    
@task()
# -> MEANS THAT THIS FUNCTION RETURNS A NOTHING
def write_gcs(path: Path, color: str, dataset_file: str) -> None:
    #  UPLOADING PARQUET FILE TO GCS
    path_gcp = Path(f"data/{color}/{dataset_file}.parquet")
    # LOAD TO THE BLOCK CREATED AT UI DASHBOARD
    gcp_block = GcsBucket.load("de-zoomcamp-gcs")
    gcp_block.upload_from_path(from_path = f"{path}", to_path = path_gcp, timeout=120)
    return
    

@flow()
# -> MEANS THAT THIS FUNCTION RETURNS NOTHING
def etl_web_to_gcs() -> None: 
    # MAIN ETL FUNCTION
    color = 'yellow'
    year = 2020
    month = 1
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    
    # FETCH IS "TRAER" IN SPANISH
    df_raw = fetch(dataset_url)
    df_clean = clean(df_raw)
    # SENDING TO GOOGLE CLOUD STORAGE
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path, color, dataset_file)
    
    # NOTICE WHEN INGESTION FINISH
    print("\n\nData ingestion into the postgres database has finished \n\n")
    
    

if __name__ == "__main__":
    # FLOW
    etl_web_to_gcs()
