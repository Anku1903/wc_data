from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
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
    ua_path = os.path.abspath('ua.txt')
    ua_list = open(ua_path,'r').readlines()


    driver_linuxpath = "/usr/bin/chromedriver/chromedriver"
    driver_path = "G:/aws/aws-scraper/chromedriver.exe"
    options = Options()
    # options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-infobars')
    options.add_argument('--start-maximized')
    options.add_argument(f'user-agent={random.choice(ua_list)}')

    driver_service = Service(executable_path=driver_path)


    if proxy:
        options.add_argument(f'--proxy-server={proxy}')

    driver = webdriver.Chrome(options=options,service=driver_service)

    item = {'url': url}

    try:
        driver.get(url)
        time.sleep(10)  # Adjust per your need

        content = driver.page_source
        soup = BeautifulSoup(content, 'html.parser')

        reviews_element = soup.select_one('body > yelp-react-root > div:nth-child(1) > div.photoHeader__09f24__nPvHp.css-1qn0b6x > div.photo-header-content-container__09f24__jDLBB.css-1qn0b6x > div.photo-header-content__09f24__q7rNO.css-2wl9y > div > div > div.arrange__09f24__LDfbs.gutter-1-5__09f24__vMtpw.vertical-align-middle__09f24__zU9sE.css-9ul5p9 > div.arrange-unit__09f24__rqHTg.arrange-unit-fill__09f24__CUubG.css-v3nuob')
        reviews = ''
        rating = ''
        category = 'None'
        website = 'none'
        if reviews_element:
            reviews_in_element = reviews_element.select_one('span:nth-child(2)').select_one('a')
            if reviews_in_element:
                reviews = reviews_in_element.get_text()

        if reviews_element:
            rating_element = reviews_element.select_one('span:nth-child(1)')
            if rating_element:
                rating = rating_element.get_text()
        
        website_element = soup.find_all('a')

        for element in website_element:
            # Check if the href attribute starts with "/biz_redir"
            if element.get('href') and element.get('href').startswith("/biz_redir"):
                website = element.text
    

        category_element = soup.select_one('body > yelp-react-root > div:nth-child(1) > div.photoHeader__09f24__nPvHp.css-1qn0b6x > div.photo-header-content-container__09f24__jDLBB.css-1qn0b6x > div.photo-header-content__09f24__q7rNO.css-2wl9y > div > div > span.css-1xfc281 > span:nth-child(1) > a')
        if website_element:
            website = website_element.get_text()
        if category_element:
            category = category_element.get_text()

        
        
        
        item["rating"] = str(rating).strip()

        item["reviews"] = str(reviews).replace("(","").replace(")","").strip()

        item["website"] = str(website).strip()
        item["subcategory"] = str(category).strip()

        print(item)

        driver.quit()

        # save_data(profile=item)
        

        # Your scraping logic here

    except Exception as e:
        print(f"Error: {str(e)}")
        item['rating'] = 'none'
        item['reviews'] = "none"
        item['website'] = "none"
        item['subcategory'] = "none"

        # save_data(profile=item)
        driver.quit()
        
    
        


def scrape_multiple_urls(urls):
    for url in urls:
        scrape_yelp_profile(url)

if __name__ == "__main__":
    start_time = time.perf_counter()

    url = "https://www.yelp.com/biz/san-antonio-flowers-san-antonio?osq=Wholesalers"

    scrape_yelp_profile(url)

    end_time = time.perf_counter()
    print(f"\nExecution time: ------ {end_time-start_time} secs")

