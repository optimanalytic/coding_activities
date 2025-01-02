# This code is part of the IBM Data Engineering Professional Certificate course on Coursera.
# © IBM Corporation. All rights reserved.

import sqlite3
import pandas as pd

# Connect to the database or create it if it doesn't exist
conn = sqlite3.connect('STAFF.db')

# Define table name and attributes
table_name = 'INSTRUCTOR'
attribute_list = ['ID', 'FNAME', 'LNAME', 'CITY', 'CCODE']

# Define the file path for the CSV
file_path = './INSTRUCTOR.csv'

# Read data from the CSV file into a Pandas DataFrame
df = pd.read_csv(file_path, names=attribute_list)

# Load the DataFrame into the database, replacing any existing table with the same name
df.to_sql(table_name, conn, if_exists='replace', index=False)
print("Table is ready")

# Query 1: View all the data in the table
query_statement = f"SELECT * FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print("All data:")
print(query_output)

# Query 2: View only the FNAME column
query_statement = f"SELECT FNAME FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print("First Names:")
print(query_output)

# Query 3: View the total number of entries in the table
query_statement = f"SELECT COUNT(*) FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print("Total Entries:")
print(query_output)

# Adding new data to the table
data_dict = {
    'ID': [100],
    'FNAME': ['John'],
    'LNAME': ['Doe'],
    'CITY': ['Paris'],
    'CCODE': ['FR']
}
data_append = pd.DataFrame(data_dict)
data_append.to_sql(table_name, conn, if_exists='append', index=False)
print("Data appended successfully")

# Query 4: Re-run the total count to verify the new row
query_statement = f"SELECT COUNT(*) FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print("Updated Total Entries:")
print(query_output)

# Close the database connection
conn.close()
print("Database connection closed")
