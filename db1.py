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
table_name = "woocommerce"
get_query = '''SELECT * from woocommerce WHERE email = '';'''

def read_data():
    df_path = os.path.abspath('woo.csv')
    df = pd.read_csv(df_path)
    df = df.astype(str)
    df.fillna('',inplace=True)
    df.replace('nan','',inplace=True)
    
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
        sql = f'''INSERT INTO {table_name} VALUES (%s, %s, %s, %s,%s);'''

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


def perform_query(query):

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
        
        cursor = connection.cursor()


        # Execute the query for each record
        cursor.execute(query)


        # Commit the transaction


        connection.commit()
        

        # Close cursor and connection
        cursor.close()
        connection.close()

    


    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL:", error)


def get_data(database_name, query):
    # PostgreSQL connection parameters
    conn_params = {
        "host": host,
        "user": user,
        "password": password,
        "dbname": database_name,
        "port": port
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



if __name__ == "__main__":
    df = 
    