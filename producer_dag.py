from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.datasets import Dataset
from config import REFERRENCE_PATH, TEST
import logging


DATA_READY = Dataset(REFERRENCE_PATH)

def generate_data():
    #logger = logging.getLogger("airflow.task")
    #logger.info("🎯 Producer: "+"-"*20)
    print("test print"+"*"*20)
    return {"status": "generated", "timestamp": datetime.now().isoformat()}

with DAG(
    dag_id='producer_dag',
    schedule='*/2 * * * *',  # Every 2 minutes for testing
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['test', 'producer'],
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    generate_task = PythonOperator(
        task_id='generate_data',
        python_callable=generate_data,
        outlets=[DATA_READY]  # This triggers consumer
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> generate_task >> end