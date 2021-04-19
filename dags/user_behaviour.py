from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

from airflow.hooks.S3_hook import S3Hook
from airflow.operators import PythonOperator

import json

from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor


# config
# local
unload_user_purchase ='./scripts/sql/filter_unload_user_purchase.sql'
temp_filtered_user_purchase = '/temp/temp_filtered_user_purchase.csv'
movie_review_local = '/data/movie_review/movie_review.csv'

# remote config
BUCKET_NAME = 'learn-de-simple'
temp_filtered_user_purchase_key= 'user_purchase/stage/{{ ds }}/temp_filtered_user_purchase.csv'

movie_clean_emr_steps = './dags/scripts/emr/clean_movie_review.json'
movie_text_classification_script = './dags/scripts/spark/random_text_classification.py'

movie_review_load = 'movie_review/load/movie.csv'


EMR_ID = 'j-3NIGFDBR2EP6H'
movie_review_load_folder = 'movie_review/load/'
movie_review_stage = 'movie_review/stage/'
text_classifier_script = 'scripts/random_text_classifier.py'



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
    "start_date": datetime(2010, 12, 1),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

dag = DAG("user_behaviour", default_args=default_args,
          schedule_interval="0 0 * * *", max_active_runs=1)


move_emr_script_to_s3 = PythonOperator(
    dag=dag,
    task_id='move_emr_script_to_s3',
    python_callable=_local_to_s3,
    op_kwargs={
        'filename': movie_text_classification_script,
        'key': 'scripts/random_text_classification.py',
    },
)

with open(movie_clean_emr_steps) as json_file:
    emr_steps = json.load(json_file)

# adding our EMR steps to an existing EMR cluster
add_emr_steps = EmrAddStepsOperator(
    dag=dag,
    task_id='add_emr_steps',
    job_flow_id=EMR_ID,
    aws_conn_id='aws_default',
    steps=emr_steps,
    params={
        'BUCKET_NAME': BUCKET_NAME,
        'movie_review_load': movie_review_load_folder,
        'text_classifier_script': text_classifier_script,
        'movie_review_stage': movie_review_stage
    },
    depends_on_past=True
)

last_step = len(emr_steps) - 1

movie_review_to_s3_stage = PythonOperator(
    dag=dag,
    task_id='movie_review_to_s3_stage',
    python_callable=_local_to_s3,
    op_kwargs={
        'filename': movie_review_local,
        'key': movie_review_load,
    },
)

# sensing if the last step is complete
clean_movie_review_data = EmrStepSensor(
    dag=dag,
    task_id='clean_movie_review_data',
    job_flow_id=EMR_ID,
    step_id='{{ task_instance.xcom_pull("add_emr_steps", key="return_value")[' + str(
        last_step) + '] }}',
    depends_on_past=True
)

[movie_review_to_s3_stage, move_emr_script_to_s3] >> add_emr_steps >> clean_movie_review_data