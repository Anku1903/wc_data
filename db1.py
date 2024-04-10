import psycopg2,os
from psycopg2 import Error
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import pandas as pd

from faker import Faker

host = "bi.cf2m8k4ao692.ap-south-1.rds.amazonaws.com"
port = "3307"
database = "leads"
user = "ankur"
password = "ankur1903"


def read_data():
    df_path = os.path.abspath('wholesale.csv')
    df = pd.read_csv(df_path)
    df.fillna('',inplace=True)
    df = df.astype(str)
    records = df.to_records(index=False)
    return records


def generate_random_data(num_records):
    fake = Faker()
    data = [(fake.name(), fake.url()) for _ in range(num_records)]
    return data

def insert_data(data):

    try:
        # Connect to the PostgreSQL database
        connection = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )

        # Create a cursor object
        cursor = connection.cursor()

        # Prepare the SQL statement
        sql = '''INSERT INTO yelp_wholesale (
    name,
    url,
    email,
    city,
    page_number,
    rating,
    reviews,
    website,
    subcategory) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);'''

        # Execute the query for each record
        cursor.executemany(sql, data)

        # Commit the transaction
        connection.commit()

        # Close cursor and connection
        cursor.close()
        connection.close()

        print(f"Successfully inserted {len(data)} records into the 'clutch' table.")

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL:", error)


def get_data(query):

    try:
        # Connect to the PostgreSQL database
        connection = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            database=database,
            password=password
        )

        # Create a cursor object
        connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = connection.cursor()


        # Execute the query for each record
        cursor.execute(query)

        # Commit the transaction


        connection.commit()
        

        # Close cursor and connection
        cursor.close()
        connection.close()

        print('query executed...\n')


    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL:", error)


if __name__ == "__main__":
    query = '''CREATE TABLE yelp_wholesale (
    name VARCHAR,
    url VARCHAR,
    email VARCHAR DEFAULT '',
    city VARCHAR,
    page_number VARCHAR,
    rating VARCHAR DEFAULT '',
    reviews VARCHAR DEFAULT '',
    website VARCHAR DEFAULT '',
    subcategory VARCHAR DEFAULT ''
);'''

    records = read_data()

    insert_data(records)