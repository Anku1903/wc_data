from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import pandas as pd
import psycopg2
import time
import os
import random

DB_HOST = "localhost"
DB_NAME = "yelp"
DB_USER = "postgres"
DB_PASSWORD = "ps190320"
TABLE_NAME = "restaurant"
GET_QUERY = '''SELECT name, min(url) as url from wholesale WHERE rating = '' GROUP BY name;'''

def scrape_yelp_profile(url, proxy=None):
    ua_list = [ # User agent list as provided earlier ]

    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument(f'user-agent={random.choice(ua_list)}')

    if proxy:
        chrome_options.add_argument(f'--proxy-server={proxy}')

    driver = webdriver.Chrome(options=chrome_options)

    item = {'url': url}

    try:
        driver.get(url)
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        time.sleep(10)  # Adjust per your need

        content = driver.page_source
        soup = BeautifulSoup(content, 'html.parser')

        # Your scraping logic here

    except Exception as e:
        print(f"Error: {str(e)}")
        item['rating'] = 'none'
        item['reviews'] = "none"
        item['website'] = "none"
        item['subcategory'] = "none"
        
    finally:
        save_data(profile=item)
        driver.quit()

def save_data(profile):
    # Database interaction as provided earlier

def get_data(database_name, query):
    # Database interaction as provided earlier

def scrape_multiple_urls(urls):
    for url in urls:
        scrape_yelp_profile(url)

if __name__ == "__main__":
    start_time = time.perf_counter()

    df = get_data(database_name='yelp', query=GET_QUERY)
    chunk_size = 5
    urls = df.head(chunk_size)["url"].tolist()

    scrape_multiple_urls(urls)

    end_time = time.perf_counter()
    print(f"\nExecution time: ------ {end_time-start_time} secs")

