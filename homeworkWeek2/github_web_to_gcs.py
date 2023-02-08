from prefect.filesystems import GitHub
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
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    print(f"columns: {df.dtypes}")
    print(f"ROWS: {len(df)}")
    return df
    

@task(log_prints=True, retries=3)
# -> MEANS THAT THIS FUNCTION RETURNS A PATH
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    # WRITE DF OUT LOCALLY AS PARQUET FILE
    path = Path(f"/home/m710/Escritorio/Data_Engineering_Zoomcamp/week2_Workflow_Orchestration/data/{color}/{dataset_file}.parquet")
    if not path.parent.is_dir():
        path.parent.mkdir(parents=True)
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
    


@flow(log_prints=True)
def github_flow() -> None:
    github_block = GitHub.load("flow-storage-block")
    github_block.get_directory("homeworkWeek2")
    # MAIN ETL FUNCTION
    color = 'green'
    year = 2019
    month = 4
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    
    # FETCH IS "TRAER" IN SPANISH
    df_raw = fetch(dataset_url)
    df_clean = clean(df_raw)
    # SENDING TO GOOGLE CLOUD STORAGE
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path, color, dataset_file)
    
    
    
    # NOTICE WHEN INGESTION FINISH
    print(f"\n\nData ingestion for {color},{year} and {month} into the postgres database has finished \n\n")
    
    
if __name__ == '__main__':
    github_flow()


