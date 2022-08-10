from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime, timedelta

# Define params for Submit Run Operator
new_cluster = {
    'spark_version': '10.4.x-scala2.12',
    'num_workers': 1,
    'init_scripts': [
        {'dbfs': {'destination': 'dbfs:/tmp/cgrant_fox.sh'}}
    ],
    'instance_pool_id': '0620-011756-votes1-pool-x2ndb56o',
    'driver_instance_pool_id': '0620-011756-votes1-pool-x2ndb56o'
}

etl_task_10 = {
    'python_file': 'file:/home/hadoop/databricks_airflow_example/spark/etl.py',
    'parameters': ['--arg1', '10']
}

etl_task_20 = {
    'python_file': 'file:/home/hadoop/databricks_airflow_example/spark/etl.py',
    'parameters': ['--arg1', '20']
}

graph_task = {
    'python_file': 'file:/home/hadoop/databricks_airflow_example/spark/agg.py',
    'parameters': ['--arg1', '20', '--arg2', '10']
}

agg_task = {
    'python_file': 'file:/home/hadoop/databricks_airflow_example/spark/agg.py',
    'parameters': ['--arg1', '10', '--arg2', '20']
}

# Define params for Run Now Operator
notebook_params = {
    "Variable": 5
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG('databricks_dag',
         start_date=datetime(2021, 1, 1),
         schedule_interval='@daily',
         catchup=False,
         default_args=default_args
         ) as dag:
    etl10 = DatabricksSubmitRunOperator(
        task_id='submit_etl10',
        databricks_conn_id='databricks',
        new_cluster=new_cluster,
        spark_python_task=etl_task_10
    )
    etl20 = DatabricksSubmitRunOperator(
        task_id='submit_etl20',
        databricks_conn_id='databricks',
        new_cluster=new_cluster,
        spark_python_task=etl_task_20
    )
    graph = DatabricksSubmitRunOperator(
        task_id='submit_graph',
        databricks_conn_id='databricks',
        new_cluster=new_cluster,
        spark_python_task=agg_task
    )
    agg = DatabricksSubmitRunOperator(
        task_id='submit_agg',
        databricks_conn_id='databricks',
        new_cluster=new_cluster,
        spark_python_task=agg_task
    )

    [etl10, etl20] >> graph >> agg