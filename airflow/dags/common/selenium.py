from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.ui import Select
from nordvpn_switcher import initialize_VPN, rotate_VPN, terminate_VPN
import time
import random

def create_driver():
    options = webdriver.ChromeOptions()
    options.binary_location = '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome'
    return webdriver.Chrome(options=options)

def quit_driver(driver):
    driver.quit()

def get_page_source(driver: webdriver, url: str, class_name: str):
    driver.get(url)
    WebDriverWait(
        driver=driver,
        timeout=300
    ).until(
        method=expected_conditions.presence_of_element_located(
            locator=(By.CLASS_NAME, class_name)
        )
    )
    page_source = driver.page_source

    return page_source

def click_element(driver: webdriver, selector: str, by: By):

    """
    For sale Xpath :"//*[@id='app']/div/main/div/div/div[1]/div[4]"
        Last 1 days: "/html/body/div[3]/div[2]/div/div[1]/div/div/span[2]"

    Sold Xpath: "//*[@id='app']/div/main/div/div/div[1]/div[5]/div[2]/span"
        Last 1 Days: "/html/body/div[4]/div[2]/div/div[1]/div/div/span[2]"

    Property type: "//*[@id='app']/div/main/div/div/div[1]/div[2]/div/span"
    Detached: "/html/body/div[3]/div[2]/div/div[2]/div/p"
    Save: "/html/body/div[3]/div[4]/div/button[2]"
    """
    driver.find_element(by, selector).click()


def click_elements(driver, elements):
    for element in elements:
        time.sleep(random.randint(10, 20))
        click_element(driver=driver, selector=element[0], by=element[1])

