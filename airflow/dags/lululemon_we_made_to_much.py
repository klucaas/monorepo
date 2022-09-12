from airflow.decorators import dag, task, virtualenv_task
from airflow.providers.dingding.operators.dingding import DingdingOperator
from datetime import datetime
import requests
import json
import time
import random


@dag(schedule_interval="@hourly", start_date=datetime(2022, 9, 9), catchup=False)
def taskflow():

    @task.virtualenv(
        task_id="generate_accessories_dataframe",
        requirements=["bs4", "nordvpn-switcher", "pandas"],
        retries=2
    )
    def check_for_belt_bags() -> str:
        from bs4 import BeautifulSoup
        from collections import Counter
        from nordvpn_switcher import initialize_VPN, rotate_VPN, terminate_VPN
        import logging
        from pandas import DataFrame

        MINI_BELT_BAG = "Mini Belt Bag"
        EVERYWHERE_BELT_BAG = "Everywhere Belt Bag"
        BASE_ACCESSORIES_URL = "https://shop.lululemon.com/api/c/accessories?page="

        exit_criteria = {MINI_BELT_BAG: None, EVERYWHERE_BELT_BAG: None}

        #vpn_setup = initialize_VPN(area_input=["Canada", "United States"])

        r = requests.get(BASE_ACCESSORIES_URL)
        #logging.info(f"Using IP:{r.json()['ip']}")

        soup = BeautifulSoup(r.content, "html.parser")
        data = soup.findAll(text=True)

        string = ""
        for d in data:
            str(d)
            string += d

        parsed = json.loads(string)

        last_page = int(parsed["links"]["last"].split("=")[1])

        for page in range(1, last_page + 1):
            time.sleep(random.randint(1, 30))
            #rotate_VPN(instructions=vpn_setup)
            r = requests.get(BASE_ACCESSORIES_URL + page)
            logging.info(f"Using IP:{r.json()['ip']} for URL:{r.url}")
            soup = BeautifulSoup(r.content, "html.parser")
            data = soup.findAll(text=True)
            for d in data:
                str(d)
                string += d

            if MINI_BELT_BAG in string:
                exit_criteria[MINI_BELT_BAG] = True

            if EVERYWHERE_BELT_BAG in string:
                exit_criteria[EVERYWHERE_BELT_BAG] = True

            if Counter(exit_criteria.values())[True] == 2:
                break


        #text = json.dumps(parsed, sort_keys=True, indent=4)
        return exit_criteria

    @task.branch(
        task_id="branch"
    )
    def choose_branch():
        return 'send_text_message'

    send_text_message = DingdingOperator(
        task_id="send_text_message",
        dingding_conn_id="dingding_default",
        message_type="markdown",
        message={
            'title': 'Airflow dingding markdown message',
            'text': '# Markdown message title\n'
                    'content content .. \n'
                    '### sub-title\n'
                    '![logo](http://airflow.apache.org/_images/pin_large.png)'
        },
        at_mobiles=['15195041469'],
        at_all=False,
    )
    check_for_belt_bags >> choose_branch >> send_text_message


dag = taskflow()
