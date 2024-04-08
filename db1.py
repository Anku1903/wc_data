import psycopg2
from psycopg2 import Error

host = "bi.cf2m8k4ao692.ap-south-1.rds.amazonaws.com"
port = "3307"
database = "woo"
user = "ankur"
password = "ankur1903"

def execute_query(query):
    # Define database connection parameters
    

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

        # Execute the query
        cursor.execute(query)

        result = cursor.fetchall()
        print(result)

        # Commit the transaction
        connection.commit()

        # Close cursor and connection
        cursor.close()
        connection.close()

        print("query executed successfully...")

    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL:", error)
        

# Example usage:
if __name__ == "__main__":
    # Replace the query below with your SQL query
    query = '''INSERT INTO clutch VALUES ('ankur','aws.com');'''
    execute_query(query)