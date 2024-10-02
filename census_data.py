import pandas as pd
from pymongo import MongoClient
import mysql.connector

# 1. Load CSV file into a pandas DataFrame
csv_file_path = 'C:/Users/AJAY/Downloads/sub-est2014_1.csv'  # Update this path
data = pd.read_csv(csv_file_path)

# Perform any necessary transformations
# Example: Replace missing values or drop irrelevant columns (if needed)
data.fillna(0, inplace=True)

# Convert DataFrame to dictionary format for MongoDB
data_dict = data.to_dict(orient='records')

# 2. Insert data into MongoDB
def load_to_mongodb(data_dict):
    client = MongoClient('mongodb://localhost:27017/')
    db = client['Population']
    collection = db['Population_data']
    
    # Insert data into MongoDB
    collection.insert_many(data_dict)
    print("Data inserted into MongoDB successfully.")

# 3. Fetch data from MongoDB and load into MySQL
def load_from_mongo_to_mysql():
    # Connect to MongoDB
    client = MongoClient('mongodb://localhost:27017/')
    db = client['Population']
    collection = db['Population_data']

    # Fetch data from MongoDB
    mongo_data = collection.find()

    # Connect to MySQL
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="raju10ajay11",
        database="population_data",
        connection_timeout=600,  # Increased timeout to handle large data inserts
        autocommit=True
    )
    cursor = conn.cursor()

    # Create table in MySQL (if not exists)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS population_status (
            SUMLEV INT,
            STATE INT,
            COUNTY INT,
            PLACE INT,
            COUSUB INT,
            CONCIT INT,
            PRIMGEO_FLAG INT,
            FUNCSTAT VARCHAR(1),
            NAME VARCHAR(255),
            STNAME VARCHAR(255),
            CENSUS2010POP INT,
            ESTIMATESBASE2010 INT,
            POPESTIMATE2010 INT,
            POPESTIMATE2011 INT,
            POPESTIMATE2012 INT,
            POPESTIMATE2013 INT,
            POPESTIMATE2014 INT
        );
    ''')

    # Insert data from MongoDB to MySQL with data validation
    for row in mongo_data:
        try:
            # Ensure CENSUS2010POP and other integer fields are valid integers
            census2010pop = int(row['CENSUS2010POP']) if str(row['CENSUS2010POP']).isdigit() else 0
            estimates_base_2010 = int(row['ESTIMATESBASE2010']) if str(row['ESTIMATESBASE2010']).isdigit() else 0
            pope_estimate_2010 = int(row['POPESTIMATE2010']) if str(row['POPESTIMATE2010']).isdigit() else 0
            pope_estimate_2011 = int(row['POPESTIMATE2011']) if str(row['POPESTIMATE2011']).isdigit() else 0
            pope_estimate_2012 = int(row['POPESTIMATE2012']) if str(row['POPESTIMATE2012']).isdigit() else 0
            pope_estimate_2013 = int(row['POPESTIMATE2013']) if str(row['POPESTIMATE2013']).isdigit() else 0
            pope_estimate_2014 = int(row['POPESTIMATE2014']) if str(row['POPESTIMATE2014']).isdigit() else 0

            cursor.execute('''
                INSERT INTO population_status (
                    SUMLEV, STATE, COUNTY, PLACE, COUSUB, CONCIT, PRIMGEO_FLAG, FUNCSTAT, NAME, STNAME,
                    CENSUS2010POP, ESTIMATESBASE2010, POPESTIMATE2010, POPESTIMATE2011, POPESTIMATE2012, POPESTIMATE2013, POPESTIMATE2014
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', (
                row['SUMLEV'], row['STATE'], row['COUNTY'], row['PLACE'], row['COUSUB'], row['CONCIT'], row['PRIMGEO_FLAG'], 
                row['FUNCSTAT'], row['NAME'], row['STNAME'], census2010pop, estimates_base_2010, pope_estimate_2010, 
                pope_estimate_2011, pope_estimate_2012, pope_estimate_2013, pope_estimate_2014
            ))
        except Exception as e:
            print(f"Error inserting row: {row}. Error: {e}")
            continue

    # Commit the transaction and close the connection
    conn.commit()
    cursor.close()
    conn.close()

    print("Data transferred from MongoDB to MySQL successfully.")

# Main execution
if __name__ == "__main__":
    load_to_mongodb(data_dict)  # Load data to MongoDB
    load_from_mongo_to_mysql()  # Transfer data from MongoDB to MySQL
