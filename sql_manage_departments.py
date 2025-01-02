# This code is part of the IBM Data Engineering Professional Certificate course on Coursera.
# © IBM Corporation. All rights reserved.

import sqlite3
import pandas as pd

# Connect to the database
conn = sqlite3.connect("STAFF.db")

# Step 1: Create the Departments table
table_name = "Departments"
attribute_list = ['DEPT_ID', 'DEP_NAME', 'MANAGER_ID', 'LOC_ID']

# Create the table with the given schema
create_table_query = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    DEPT_ID INTEGER,
    DEP_NAME TEXT,
    MANAGER_ID INTEGER,
    LOC_ID TEXT
);
"""
conn.execute(create_table_query)
print(f"Table {table_name} created successfully.")

# Step 2: Populate the Departments table from the CSV file
file_path = "./Departments.csv"  # Ensure Departments.csv is in the same directory
df = pd.read_csv(file_path, names=attribute_list)

# Load the data into the Departments table
df.to_sql(table_name, conn, if_exists='replace', index=False)
print("Data loaded successfully into the Departments table.")

# Step 3: Append additional data to the Departments table
data_dict = {
    'DEPT_ID': [9],
    'DEP_NAME': ['Quality Assurance'],
    'MANAGER_ID': [30010],
    'LOC_ID': ['L0010']
}
data_append = pd.DataFrame(data_dict)
data_append.to_sql(table_name, conn, if_exists='append', index=False)
print("Additional data appended successfully.")

# Step 4: Queries
# a. View all entries
query_statement = f"SELECT * FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print("All entries:")
print(query_output)

# b. View only the department names
query_statement = f"SELECT DEP_NAME FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print("Department Names:")
print(query_output)

# c. Count the total entries
query_statement = f"SELECT COUNT(*) FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print("Total Entries:")
print(query_output)

# Close the connection
conn.close()
print("Database connection closed.")
