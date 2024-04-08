import psycopg2
from psycopg2 import Error

from faker import Faker

host = "bi.cf2m8k4ao692.ap-south-1.rds.amazonaws.com"
port = "3307"
database = "woo"
user = "ankur"
password = "ankur1903"



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
        sql = "INSERT INTO clutch (name, url) VALUES (%s, %s)"

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

if __name__ == "__main__":
    # Number of records to generate and insert
    num_records = 30

    # Generate random data
    data = generate_random_data(num_records)

    # Insert data into PostgreSQL
    insert_data(data)
