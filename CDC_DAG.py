'''
The code abstact:
-- DAG is to run the code pipeline at certain time interval
-- In the following code it is set as hourly
-- refer https://tinyurl.com/schedulersyntax for details

-- importing libs
-- DAG_NAME = code name (the dag is code if your dag-code is code.py)
-- defining default args parameters. more on https://tinyurl.com/dagargs
-- creating a DAG with variable named dag which will be used later in the code
    - for ease of understanding and code the name is same 
-- func get_encemble_records: 
    - getting data from manually created bucket in Google Cloud Storage(jeet_cdc in this case) because it is dumped by pub/sub topic via cloud function
    - creating a file name combined.csv in data lib in bucket created from cloud composer
-- func upload_to_bigquery:
    - writing the data into bigquery table using its table id
    - the job_config has skip_leading_rows=1 to make sure the heading/column names are not registered as data points
-- the last segment of code is configuring the steps using airflow operators
-- the path is given at the end of the code

for query contact me on swadiajeet@gmail.com
'''



import pandas as pd
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from google.cloud import storage
from google.cloud import bigquery


DAG_NAME = 'CDC_DAG'

default_args = {
    "depends_on_past": False,
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "start_date": datetime(2022,1,1,0,0,0)
}

dag = DAG(
    dag_id=DAG_NAME,
    default_args=default_args,
    schedule_interval= '@hourly',
    catchup=False,
    description=DAG_NAME,
    max_active_runs=1
)

def get_ensemble_records():
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('jeet_cdc')
    blobs = bucket.list_blobs()
    combined_df = pd.DataFrame()

    # downloading all csv files from the google cloud storage bucket and combining it into one dataframe
    for blob in blobs:
        filename = blob.name.replace('/', '_')

        blob.download_to_filename(f'/home/airflow/gcs/data/covid_processed/{filename}') #CHECK THE PATH
        read_file_df = pd.read_csv(f'/home/airflow/gcs/data/covid_processed/{filename}')#CHECK THE PATH
        combined_df = combined_df.append(read_file_df)

        # deleting the csv file from google cloud storage bucket
        print(f"Deleting file {filename}")
        blob.delete()

    # return combined_df as csv
    if len(combined_df) > 0:
        ensembled_file_name = f"combined_files.csv"
        combined_df.to_csv(f"/home/airflow/gcs/data/covid_processed/{ensembled_file_name}", index=False)#CHECK THE PATH
        
        return "Processed_records"
    else:
        
        return "completed"


def upload_to_bigquery():
    client = bigquery.Client()

    table_id = 'cdcjeet.jeetcdc.jeetcdc_table'
    destination_table = client.get_table(table_id)

    row_count_before_inserting = destination_table.num_rows
    

    if row_count_before_inserting > 0:
        disposition = bigquery.WriteDisposition.WRITE_APPEND
       
    elif row_count_before_inserting == 0:
        disposition = bigquery.WriteDisposition.WRITE_EMPTY
       
    job_config = bigquery.LoadJobConfig(
        write_disposition=disposition,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True
    )

    uri = f'gs://us-central1-envcdcjeet-735c9b61-bucket/data/covid_processed/combined_files.csv' #CHECK THE URL
    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )
    load_job.result()

start = DummyOperator(
    task_id="started",
    dag=dag)

extracting = PythonOperator(
    task_id='ensemble_records',
    python_callable=get_ensemble_records,
    dag=dag
)

uploading_to_bq = PythonOperator(
    task_id='upload_records_to_bigquery',
    python_callable=upload_to_bigquery,
    dag=dag
)

completed = DummyOperator(
    task_id="completed",
    dag=dag)

start >> extracting >> uploading_to_bq >> completed
