from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
import os

@task(log_prints=True) 
# -> MEANS THAT THIS FUNCTION RETURNS A DATAFRAME
#  FETCH IS EXPECTING A DATASET_URL VARIABLE OF STRING TYPE
def fetch(dataset_url: str) -> pd.DataFrame:
    # READ DATA FROM WEB 
    df = pd.read_csv(dataset_url)
    return df 
    
@task(log_prints=True)
def clean(df: pd.DataFrame):
    #  FIXING DTYPE DATETIME ISSUES
    df.pickup_datetime = pd.to_datetime(df.pickup_datetime)
    df.dropOff_datetime = pd.to_datetime(df.dropOff_datetime)
    df['PUlocationID'] = df['PUlocationID'].astype('Int64')
    df['DOlocationID'] = df['DOlocationID'].astype('Int64')
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df
    

@task(log_prints=True)
# -> MEANS THAT THIS FUNCTION RETURNS A PATH
def write_local(df: pd.DataFrame, year: str, month: str) -> Path:
    # WRITE DF OUT LOCALLY AS CSV FILE
    dataset_path = f"fhv_tripdata_{year}-{month:02}"
    path = Path(f"data/{dataset_path}.parquet")
    if not path.parent.is_dir():
        path.parent.mkdir(parents=True)
    df.to_parquet(path, compression="gzip")
    return path  
    
    
@task(log_prints=True)
def write_gcs(path: Path) -> None:
    # LOAD TO THE BLOCK CREATED AT UI DASHBOARD
    gcp_block = GcsBucket.load("de-zoomcamp-gcs")
    gcp_block.upload_from_path(from_path = f"{path}", to_path = path, timeout = 120)
    return
    

@flow(log_prints=True)
# -> MEANS THAT THIS FUNCTION RETURNS NOTHING
def main_flow(log_prints=True) -> None: 
    # MAIN ETL FUNCTION
    year = 2019
    csv_list = []
    ROWS = 0
    for month in range(1,13):
        dataset_file = f"fhv_tripdata_{year}-{month:02}"
        dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}.csv.gz"
    
        # FETCH IS "TRAER" IN SPANISH
        df_raw = fetch(dataset_url)
        df_clean = clean(df_raw)
        ROWS += len(df_clean)
        # STORING LOCALLY THE CSV.GZ FILES
        path = write_local(df_clean, year, month)
        write_gcs(path)
        
    print(f"\n\nThe total of processed rows from web to local storing are: {ROWS} rows\n\n")
    # NOTICE WHEN INGESTION FINISH
    print("\n\nData ingestion into the postgres database has finished \n\n")
    
    

if __name__ == "__main__":
    # FLOW
    main_flow()
