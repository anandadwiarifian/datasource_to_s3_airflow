# datasource_to_s3_airflow
Simulation of filtering and data pulling by date range from a data source to S3 bucket, scheduled by airflow

The project simulates retail sales data from an online shop to S3 bucket.
The data being pulled are filtered first, based on the data range.

I use docker services to create 2 containers: airflow and postgres

Airflow is used to schedule the process.
Postgres is used to act as the data source of the retail sales data.

## How to use
First, run the containers by running this code.
`docker-compose -f docker-compose-LocalExecutor.yml up -d`

The docker should be up and running now. Then, create a postgres connection using pgcli
`pgcli -h localhost -p 5432 -U airflow -d airflow`
Note that the port, username, and database name match with the ones in [docker-compose.yml](datasource_to_s3_airflow/docker-compose.yml), for postgres container.

Next, create a scheme: retail, table: user_purchase, and import the data from `/setup/raw_input_data/retail/OnlineRetail.csv` 
Or just execute the [sql script](datasource_to_s3_airflow/setup/postgres/create_user_purchase.sql) by run this code in the `pg` session.
`\i ~/beginner_de_project/setup/postgres/create_user_purchase.sql`

The pg table are now ready to act as the data source.

### Airflow
You can see the script for the DAG with comments [here](datasource_to_s3_airflow/dags/user_behaviour.py).
Overall, the flow is like this
- pg_unload
The data in the data source is filtered based on the execution of the script (for this project, the date is set to 2010-12-01) by running this sql [script](datasource_to_s3_airflow/scripts/sql/filter_unload_user_purchase.sql). The `{{ }}` variables are airflow macro variables that are set in the dag file.
The filtered data is saved to local drive in a csv.
- user_purchase_to_s3_stage
The data in the csv file then pushed to S3 bucket
- remove_local_user_purchase_file
After the data successfully psuhed to S3 bucket, the csv is deleted from the local drive.
- end_of_data_pipeline
The final task that doesn't do anything (dummy task)

The screenshot of the airflow GUI:
![Stage 1 Graph View](https://user-images.githubusercontent.com/47022822/115257772-9c84a780-a15a-11eb-8e9a-dfbc65dc55b7.PNG)
![Stage 1 Tree View 1](https://user-images.githubusercontent.com/47022822/115257815-a60e0f80-a15a-11eb-9c32-f454fe5adb99.PNG)

The screenshot of S3 bucket:
![Stage 1 S3 Bucket](https://user-images.githubusercontent.com/47022822/115257916-baeaa300-a15a-11eb-86c3-e03b2345609b.PNG)



