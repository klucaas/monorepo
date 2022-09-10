from airflow.decorators import dag, task
from datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd
import requests
import json


base_url = "https://shop.lululemon.com/api/c/accessories?page="


@dag(schedule_interval="@hourly", start_date=datetime(2022, 9, 9), catchup=False)
def taskflow():

    @task(task_id="generate_accessories_wmtm_dataframe", retries=2)
    def generate_accessories_wmtm_dataframe() -> str:
        df = pd.DataFrame(columns=["Category", "Item Category", "Display Name", "Color", "URL", "Sale Price", "Original Price", "Sizes In Stock", "Sizes OOS"])
        base_accessories_url = "https://shop.lululemon.com/api/c/accessories?page="
        r = requests.get(base_accessories_url)
        soup = BeautifulSoup(r.content, "html.parser")
        data = soup.findAll(text=True)

        string = ""
        for d in data:
            str(d)
            string += d

        parsed = json.loads(string)
        text = json.dumps(parsed, sort_keys=True, indent=4)
        return text

    generate_accessories_wmtm_dataframe()


dag = taskflow()
