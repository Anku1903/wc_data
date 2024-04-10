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

DB_HOST = "bi.cf2m8k4ao692.ap-south-1.rds.amazonaws.com"
DB_NAME = "leads"
DB_USER = "ankur"
DB_PASSWORD = "ankur1903"
DB_PORT = '3307'
TABLE_NAME = "yelp_wholesale"
GET_QUERY = f'''SELECT url from {TABLE_NAME} WHERE website = '' GROUP BY url;'''

def scrape_yelp_profile(url, proxy=None):
    ua_path = os.path.abspath('ua.txt')
    ua_list = open(ua_path,'r').readlines()


    driver_linuxpath = "/usr/bin/chromedriver/chromedriver"
    driver_path = "G:/aws/aws-scraper/chromedriver.exe"
    options = Options()
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-infobars')
    options.add_argument('--start-maximized')
    options.add_argument(f'user-agent={random.choice(ua_list)}')

    driver_service = Service(executable_path=driver_linuxpath)


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
        
        if category_element:
            category = category_element.get_text()

        
        
        
        item["rating"] = str(rating).strip()

        item["reviews"] = str(reviews).replace("(","").replace(")","").strip()

        item["website"] = str(website).strip()
        item["subcategory"] = str(category).strip()


        driver.quit()

        save_data(item)
        

        # Your scraping logic here

    except Exception as e:
        print(f"Error: {str(e)}")
        item['rating'] = 'none'
        item['reviews'] = "none"
        item['website'] = "none"
        item['subcategory'] = "none"

        save_data(item)
        driver.quit()
        
    
   
def save_data(data):

    try:
        # Connect to the PostgreSQL database
        connection = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )

        # Create a cursor object
        cursor = connection.cursor()

        # Prepare the SQL statement
        sql = f'''UPDATE {TABLE_NAME} SET rating = %s,reviews = %s,website = %s,subcategory = %s WHERE url = %s;'''

        # Execute the query for each record
        values = (data['rating'],data['reviews'],data['website'],data['subcategory'],data['url'])
        cursor.execute(sql, values)

        # Commit the transaction
        connection.commit()

        # Close cursor and connection
        cursor.close()
        connection.close()

        print(f"Update Successfully...")

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL:", error)

     


def get_data(database_name, query):
    # PostgreSQL connection parameters
    conn_params = {
        "host": DB_HOST,
        "user": DB_USER,
        "password": DB_PASSWORD,
        "dbname": database_name,
        "port": DB_PORT
    }

    # Establish a connection to the PostgreSQL database
    try:
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()

        # Execute the query
        cursor.execute(query)

        # Fetch all data from the query result
        data = cursor.fetchall()

        # Get column names
        col_names = [desc[0] for desc in cursor.description]

        # Create a Pandas DataFrame
        df = pd.DataFrame(data, columns=col_names)

        # Close cursor and connection
        cursor.close()
        conn.close()

        return df

    except psycopg2.Error as e:
        print("Error connecting to PostgreSQL:", e)
        return None


def scrape_multiple_urls(urls):
    for url in urls:
        scrape_yelp_profile(url)
        time.sleep(random.uniform(1,4))

if __name__ == "__main__":
    start_time = time.perf_counter()

    df = get_data(database_name=DB_NAME,query=GET_QUERY)

    url_size = 500
    urls = df.head(url_size)['url'].tolist()

    scrape_multiple_urls(urls)

    end_time = time.perf_counter()
    print(f"\nExecution time: ------ {end_time-start_time} secs")

