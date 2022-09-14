from airflow.decorators import dag, task, virtualenv_task, branch_task
from airflow.operators.empty import EmptyOperator
from typing import Dict
from datetime import datetime


@dag(schedule_interval="0 1/2 * * *", start_date=datetime(2022, 9, 9), catchup=False)
def taskflow():

    @task.virtualenv(
        task_id="check_for_belt_bags",
        requirements=["bs4", "nordvpn-connect", "pandas"],
        retries=2,
    )
    def check_for_belt_bags():
        from bs4 import BeautifulSoup
        from collections import Counter
        from nordvpn_connect import initialize_vpn, rotate_VPN, close_vpn_connection
        from airflow.kubernetes.secret import Secret
        import logging
        import requests
        import json
        import time
        import random
        import os


        NORD_USER = Secret(deploy_type="env", deploy_target="NORD_USER", secret="airflow-secrets", key="NORD_USER")
        NORD_PASSWORD = Secret(deploy_type="env", deploy_target="NORD_PASSWORD", secret="airflow-secrets",
                               key="NORD_PASSWORD")

        MINI_BELT_BAG = "Mini Belt Bag"
        EVERYWHERE_BELT_BAG = "Everywhere Belt Bag"
        BASE_ACCESSORIES_URL = "https://shop.lululemon.com/api/c/accessories?page="

        exit_criteria = {MINI_BELT_BAG: None, EVERYWHERE_BELT_BAG: None}

        os.system("sh <(wget -qO - https://downloads.nordcdn.com/apps/linux/install.sh)")

        vpn_setup = initialize_vpn("United States", NORD_USER, NORD_PASSWORD)
        rotate_VPN(vpn_setup)

        r = requests.get(BASE_ACCESSORIES_URL, stream=True)
        server_ip, server_port = r.raw._connection.sock.getpeername()
        external_ip = requests.get('https://checkip.amazonaws.com').text.strip()
        logging.info(f"Using Server IP:{server_ip}:{server_port} and External IP:{external_ip} for URL:{r.url}")

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

            r = requests.get(BASE_ACCESSORIES_URL + str(page), stream=True)
            server_ip, server_port = r.raw._connection.sock.getpeername()
            external_ip = requests.get('https://checkip.amazonaws.com').text.strip()
            logging.info(f"Using Server IP:{server_ip}:{server_port} and External IP:{external_ip} for URL:{r.url}")
            soup = BeautifulSoup(r.content, "html.parser")
            data = soup.findAll(text=True)
            for d in data:
                str(d)
                string += d

            if MINI_BELT_BAG in string:
                exit_criteria[MINI_BELT_BAG] = True
                logging.info(f"Found {MINI_BELT_BAG} on {BASE_ACCESSORIES_URL + str(page)}")

            if EVERYWHERE_BELT_BAG in string:
                exit_criteria[EVERYWHERE_BELT_BAG] = True
                logging.info(f"Found {EVERYWHERE_BELT_BAG} on {BASE_ACCESSORIES_URL + str(page)}")

            if Counter(exit_criteria.values())[True] == 2:
                break

        close_vpn_connection()
        #text = json.dumps(parsed, sort_keys=True, indent=4)
        return exit_criteria

    @task.branch(
        task_id="choose_branch"
    )
    def choose_branch(result: Dict[str, bool]):
        if True in result.values():
            return "send_text_message"
        return "skip"

    skip = EmptyOperator(task_id="skip")

    @task.virtualenv(
        task_id="send_text_message",
        requirements=["twilio"],
        retries=2,
    )
    def send_text_message(**kwargs):
        import os
        import logging
        from twilio.rest import Client
        from airflow.kubernetes.secret import Secret

        TWILIO_ACCOUNT_SID = Secret(deploy_type="env", deploy_target="TWILIO_ACCOUNT_SID", secret="airflow-secrets",
                                key="TWILIO_ACCOUNT_SID")
        TWILIO_AUTH_TOKEN = Secret(deploy_type="env", deploy_target="TWILIO_AUTH_TOKEN", secret="airflow-secrets",
                              key="TWILIO_AUTH_TOKEN")

        TEXT_RECIPIENT = Secret(deploy_type="env", deploy_target="TEXT_RECIPIENT", secret="airflow-secrets",
                              key="TEXT_RECIPIENT")
        TWILIO_NUMBER = Secret(deploy_type="env", deploy_target="TWILIO_NUMBER", secret="airflow-secrets",
                                 key="TWILIO_NUMBER")

        logging.info(f"{kwargs['ti'].xcom_pull(task_ids='check_for_belt_bags')}")

        client = Client()
        client.messages.create(body="this is a test message", from_=os.environ["TWILIO_NUMBER"], to=os.environ["TEXT_RECIPIENT"])

    choose_branch(check_for_belt_bags()) >> [send_text_message(), skip]


dag = taskflow()
