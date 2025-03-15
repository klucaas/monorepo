
"""
Install UV
curl -LsSf https://astral.sh/uv/install.sh | sh

# CCXT (CryptoCurrency eXchange Trading Library)
https://pypi.org/project/ccxt/


export AIRFLOW_HOME=~/monnorepo/airflow


Idea Ingestion:
For every hour, extract the past hour of trading data.
For every hour, extract the price of each coin.


Ideas Analytics:
Try to corrrelate crypto with some other event to form a predictor
"""

from airflow.decorators import dag, task
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.models import Param
from airflow.operators.python import PythonOperator
import requests

@dag(
    dag_id="job_searcher",
    schedule="@daily",
    start_date=datetime(2024, 1, 6),
    catchup=False,
    params={"ignore_jobs": Param}
)
def daily_job_term_search():

    start = EmptyOperator(task_id="start")
    websites = [
        "https://jobs.lever.co/super-com",
        "https://www.faire.com/en-ca/careers/openings",
        "https://jobs.lever.co/1password",
        "https://jobs.lever.co/fullscript/"
    ]

    tasks = []

    """
    Task 1 - Start
    
    """
    @task
    def make_web_call(website: str):
        keywords = ["analytics engineer", "data engineer"]
        text = requests.get(website).text
        hits = []
        for keyword in keywords:
            if keyword in text:
                hits.append(keyword)
        return hits

dag = daily_job_term_search()
