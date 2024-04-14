from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from concurrent.futures import ThreadPoolExecutor, process, wait

import pandas as pd
import re
import random
import os
import time
import psycopg2


DB_HOST = "bi.cf2m8k4ao692.ap-south-1.rds.amazonaws.com"
DB_NAME = "leads"
DB_USER = "ankur"
DB_PASSWORD = "ankur1903"
DB_PORT = '3307'
TABLE_NAME = "woocommerce"
GET_QUERY = '''SELECT * from woocommerce WHERE email = '';'''


def start_scraping(url):
    ua_path = os.path.abspath('ua.txt')
    ua_list = open(ua_path,'r').readlines()
    item = {}
    custom_user_agent = random.choice(ua_list)
    # driver_path = os.path.abspath('chromedriver.exe')
    driver_linuxpath = "/usr/bin/chromedriver/chromedriver"
    options = Options()
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-infobars')
    options.add_argument('--start-maximized')
    options.add_argument(f'user-agent={custom_user_agent}')

    driver_service = Service(executable_path=driver_linuxpath)

    driver = webdriver.Chrome(
        options=options,
        service=driver_service
    )
    try:
        driver.get(url)
        time.sleep(10)

        pattern = r'\b[A-Za-z][A-Za-z0-9]*@[A-Za-z0-9.-]+\.(?!png|jpg|jpeg|gif)[A-Za-z]{2,4}\b'
        item['website'] = url

        first_page_content = driver.page_source

        fb_page_url = None
        try:
            fb_page_url = driver.find_element(By.CSS_SELECTOR, 'a[href*="facebook.com"]').get_attribute('href')
        except Exception as e:
            pass

        contact_page = None
        try:
            contact_page = driver.find_element(By.CSS_SELECTOR, 'a[href*="contact"]').get_attribute('href')
        except Exception as e:
            pass

        if fb_page_url:
            driver.get(fb_page_url)
            time.sleep(12)
            try:
                driver.find_element(By.CSS_SELECTOR, 'div[aria-label="Close"]').click()
            except:
                pass

            content = driver.page_source
            email_match_fb = re.findall(pattern, content)
            if len(email_match_fb) > 0:
                item["email"] = str(email_match_fb[0])
                save_data(item)
                driver.close()
                return item
            else:
                item["email"] = "None"

        if contact_page:
            driver.get(contact_page)
            time.sleep(10)

            email_tag = None
            try:
                email_tag = driver.find_element(By.CSS_SELECTOR, 'a[href*="mailto"]').get_attribute('href')
                if email_tag is not None:
                    item['email'] = str(email_tag).replace('mailto:', '')
                    save_data(item)

                    driver.close()
                    return item
            except Exception as e:
                pass

            contact_page_content = driver.page_source

            if item.get('email', '-1') != 'None':
                
                fb_page_url = None
                try:
                    fb_page_url = driver.find_element(By.CSS_SELECTOR, 'a[href*="facebook.com"]').get_attribute('href')
                    
                    if fb_page_url is not None:
                        driver.get(fb_page_url)
                        time.sleep(12)
                        try:
                            driver.find_element(By.CSS_SELECTOR, 'div[aria-label="Close"]').click()
                        except:
                            pass

                        content = driver.page_source
                        email_match_fb = re.findall(pattern, content)
                        if len(email_match_fb) > 0:
                            item["email"] = email_match_fb[0]
                            save_data(item)

                            driver.close()
                            return item
                        else:
                            item["email"] = "None"
                    else:
                        item['email'] = 'None'
                except Exception as e:
                    item['email'] = 'None'
                   
                    

            if item.get('email', '-1') == 'None':
                
                email_match_regex = re.findall(pattern, contact_page_content)
                email_match_first_page = re.findall(pattern, first_page_content)
                if len(email_match_regex) > 0:
                    item['email'] = str(email_match_regex[0])
                elif len(email_match_first_page) > 0:
                    item['email'] = str(email_match_first_page[0])
                else:
                    item['email'] = 'None'
                save_data(item)

                driver.close()
                return item

        else:
            item['email'] = "None"
            save_data(item)

            driver.close()
            return item

        driver.quit()

    except Exception as e:
        item['email'] = 'None'
        item['website'] = url
        save_data(item)

        driver.quit()
        return item


def save_data(item):
                 
        try:
            # Connect to the PostgreSQL database
            conn = psycopg2.connect(
                host=DB_HOST,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                port=DB_PORT
            )
            print("Connected to the database successfully.")

            # Create a cursor object to execute SQL queries
            cur = conn.cursor()

            try:
                # Construct the SQL query for data insertion and execute it
            
                query = f"UPDATE {TABLE_NAME} SET email = %s WHERE website = %s;"
                
                cur.execute(query,(item.get('email'),item.get('website')))
                conn.commit()
            except Exception as e:
                conn.rollback()  # Rollback the transaction if an error occurs
                print("Error: Unable to insert data.")
                print(e)

        except Exception as e:
            print("Error: Unable to connect to the database.")
            print(e)

        finally:
            # Close the cursor and the database connection
            cur.close()
            conn.close()

def scrape_multiple(urls):
    for url in urls:
        start_scraping(url)

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


if __name__=='__main__':
    start = time.perf_counter()
    
    df = get_data(database_name=DB_NAME,query=GET_QUERY)

    url_limit = 2
    urls = df.head(url_limit)['website'].tolist()
    
    threadlist = []

    with ThreadPoolExecutor() as executor:
        for item in urls:
            threadlist.append(executor.submit(start_scraping,item))
        
        wait(threadlist)
    

    end = time.perf_counter()
    print(f"executed in {end-start} seconds...\n")


