import airflow.utils.dates
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
#from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from data_pipeline2 import *

def etl():
    pipeline = Pipeline()
    pipeline.create_spark_session()
    pipeline.run_pipeline()


dag = DAG(
    dag_id="etl_project",
    description="etl",
    start_date=airflow.utils.dates.days_ago(5),
    schedule_interval=None,
)

bashop = BashOperator(task_id = "print", 
                                bash_command="echo 'PGDBDA'",
                                dag=dag
                                )

##define the etl task
etl_task = PythonOperator(task_id="etl_task",
                          python_callable = etl,
                          dag=dag)



bashop >> etl_task
