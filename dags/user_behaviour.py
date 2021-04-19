from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

from airflow.hooks.S3_hook import S3Hook
from airflow.operators import PythonOperator

# config
# local
unload_user_purchase ='./scripts/sql/filter_unload_user_purchase.sql'
temp_filtered_user_purchase = '/temp/temp_filtered_user_purchase.csv'

# remote config
BUCKET_NAME = 'learn-de-simple' # change it to your S3 bucket name
temp_filtered_user_purchase_key= 'user_purchase/stage/{{ ds }}/temp_filtered_user_purchase.csv' 


# helper function(s)
def _local_to_s3(filename, key, bucket_name=BUCKET_NAME):
    s3 = S3Hook()
    
    s3.load_file(filename=filename, bucket_name=bucket_name,
                 replace=True, key=key)

def remove_local_file(filelocation):
    if os.path.isfile(filelocation):
        os.remove(filelocation)
    else:
        logging.info(f'File {filelocation} not found')


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2010, 12, 1),  # the start date of the DAG. This will be used as an argument to the sql script as a macro variable ds
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

# below initialize a DAG named user_behaviour with default_arg above, schedule interval = 0 0 * * * (0 minute 0 hour), and max active runs = 1 to prevent overlapping runs
dag = DAG("user_behaviour", default_args=default_args,
          schedule_interval="0 0 * * *", max_active_runs=1)

# dummy task
end_of_data_pipeline = DummyOperator(task_id='end_of_data_pipeline', dag=dag)

# below is the step to filter the data from the data source by executing the sql script which referenced by unload_user_purchase. 
# the filtered data is saved as a csv in temp_filtered_user_purchase.
pg_unload = PostgresOperator( 
    dag=dag,
    task_id='pg_unload',
    sql=unload_user_purchase,
    postgres_conn_id='postgres_default',
    params={'temp_filtered_user_purchase': temp_filtered_user_purchase},
    depends_on_past=True,
    wait_for_downstream=True
)

# the filtered data then are pushed to S3 using the local_to_s3 function
user_purchase_to_s3_stage = PythonOperator(
    dag=dag,
    task_id='user_purchase_to_s3_stage',
    python_callable=_local_to_s3,
    op_kwargs={
        'filename': temp_filtered_user_purchase, # the csv of filtered data
        'key': temp_filtered_user_purchase_key, # the directory of the filtered data in the S3 bucket
    },
)

# delete the csv of filtered data in the local drive
remove_local_user_purchase_file = PythonOperator(
    dag=dag,
    task_id='remove_local_user_purchase_file',
    python_callable=remove_local_file,
    op_kwargs={
        'filelocation': temp_filtered_user_purchase,
    },
)

# this is the flow of the DAG
pg_unload >> user_purchase_to_s3_stage >> remove_local_user_purchase_file >> end_of_data_pipeline
