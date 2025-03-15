"""
Install UV
curl -LsSf https://astral.sh/uv/install.sh | sh

export AIRFLOW_HOME=~/monorepo/airflow
"""

from airflow.decorators import dag, task
from datetime import datetime
from selenium.webdriver.common.by import By
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from dags.common.selenium import get_page_source, create_driver, click_elements
from nordvpn_switcher import initialize_VPN, rotate_VPN, terminate_VPN
import time
import random
from bs4 import BeautifulSoup

@dag(
    dag_id="housesigma",
    schedule="@daily",
    start_date=datetime(2024, 1, 6),
    catchup=False
)
def scrape():

    @task
    def connect_and_test_vpn():
        initialize_VPN(stored_settings=1)
        rotate_VPN()


    """
    Manual Config - Full Scrape
    Default - Last 1 day
    
    """

    @task
    def request():
        captured_html = {}
        current_page = 1
        listings_class_name = "listings"

        url = (
            "https://housesigma.com/on/kitchener-real-estate/map/?"
            "center_marker=43.4516395,-80.4925337"
            "&view=list"
            "&municipality=10164"
            "&status=for-sale,sold"
            "&lat=43.444530&lon=-80.493981"
            "&zoom=11"
            f"&page={current_page}"
        )

        #if context.get('dag_run').external_trigger:
        driver = create_driver()
        _ = get_page_source(driver, url=url, class_name=listings_class_name)
        click_elements(
            driver,
   [
                ("//*[@id='app']/div/main/div/div/div[1]/div[4]", By.XPATH), # For Sale Dropdown
                ("/html/body/div[3]/div[2]/div/div[1]/div/div/span[2]", By.XPATH), # Last 1 days
                ("//*[@id='app']/div/main/div/div/div[1]/div[5]/div[2]/span", By.XPATH), # Sold Dropdown
                ("/html/body/div[4]/div[2]/div/div[1]/div/div/span[2]", By.XPATH), # Last 1 Days
                ("//*[@id='app']/div/main/div/div/div[1]/div[2]/div/span", By.XPATH), # Property Type Dropdown
                ("/html/body/div[3]/div[2]/div/div[2]/div/p", By.XPATH), # Detached
                ( "/html/body/div[3]/div[4]/div/button[2]", By.XPATH), # Save
            ]
        )

        html = driver.page_source
        captured_html.update({1: html})

        soup = BeautifulSoup(html)
        page_container = soup.find("div", class_="app-pagination")
        last_page = int(page_container.find_all("span")[-2].text)


        for page in range(current_page + 1, last_page + 1):
            time.sleep(random.randint(60, 180))
            url = url.replace(f"page={current_page}", f"page={page}")
            captured_html.update({current_page: get_page_source(driver=driver, url=url, class_name=listings_class_name)})
            current_page += 1


        hook = GCSHook(conn="gcs_conn")
        hook.upload(
            bucket_name="",
            object_name="",
            data=captured_html
        )


        






        # listings = soup.find("div", class_="listings")
        # cards = soup.find_all("article", class_="pc-listing-card")
        # pages = soup.



scrape()
