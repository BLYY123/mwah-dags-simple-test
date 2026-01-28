from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.datasets import Dataset
from config import REFERRENCE_PATH
import logging

DATA_READY = Dataset(REFERRENCE_PATH)

def process_data(**context):
    #logger = logging.getLogger("airflow.task")
    #logger.info("✅ Consumer:"+"-"*20)
    print("test print"+"*"*20)
    return {"status": "processed", "triggered_at": datetime.now().isoformat()}

with DAG(
    dag_id='consumer_dag',
    schedule=[DATA_READY],  # Triggered by dataset
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['test', 'consumer'],
) as dag:
    
    process_task = PythonOperator(
        task_id='process_data',
        python_callable=process_data,
    )
    
    process_task