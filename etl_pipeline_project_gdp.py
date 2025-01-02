# This code is part of the IBM Data Engineering Professional Certificate course on Coursera.
# © IBM Corporation. All rights reserved.

import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import re
import sqlite3
import json
import matplotlib.pyplot as plt

# Log function to track process
log_file = "etl_project_log.txt"
def log_progress(message):
    with open(log_file, "a") as f:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        f.write(f"{timestamp} - {message}\n")

# Step 1: Extract data from the webpage
log_progress("Starting data extraction.")
url = "https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29"
response = requests.get(url)
soup = BeautifulSoup(response.content, "html.parser")
tables = soup.find_all("table", {"class": "wikitable"})

if not tables:
    log_progress("No tables found on the webpage.")
    raise Exception("No tables found on the webpage.")

# Select the correct table
table = tables[0]  # Adjust index if necessary

# Step 2: Extract and process headers (handling nested headers)
headers = []
for th in table.find_all("tr")[0].find_all("th"):  # Get header row
    col_span = int(th.get("colspan", 1))
    header_text = th.text.strip()
    headers.extend([header_text] * col_span)  # Repeat header for colspan

# Clean headers by removing bracketed text like [1][13]
clean_headers = [re.sub(r'\[\d+\]', '', header).strip() for header in headers]
print(f"Cleaned Headers: {clean_headers}")

# Step 3: Process the table rows into a DataFrame
log_progress("Processing data into a DataFrame.")
rows = table.find_all("tr")
data = []
for row in rows[1:]:  # Skip the header row
    cols = row.find_all("td")
    if len(cols) == len(clean_headers):  # Match number of columns to headers
        row_data = [re.sub(r'\[.*?\]', '', col.text.strip()) for col in cols]  # Clean cell values
        data.append(row_data)

# Create DataFrame
df = pd.DataFrame(data, columns=clean_headers)

# Step 4: Rename the columns explicitly
df.columns = [
    'Country/Territory', 'UN region',
    'IMF', 'IMF_year',
    'World Bank', 'World Bank_year',
    'United Nations', 'United Nations_year'
]
print(df['IMF'].head())  # View the first few entries
print(df['IMF'].apply(type).unique())  # Check the types of the column values
print('..............................')
print("Renamed Columns:")
print(df.columns)

df['IMF'] = df['IMF'].str.replace(',', '').astype(float)
print(df['IMF'].apply(type).unique())  # Check the types of the column values

# Step 4: Display all columns in terminal
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.width', 1000)       # Increase width for better visibility
print("Extracted DataFrame:")
print(df.head())

# Save the DataFrame to a CSV file for validation
df.to_csv("extracted_gdp_data_cleaned.csv", index=False)

# Step 5: Make column names unique
df.columns = [f"{col}_{i}" if df.columns.tolist().count(col) > 1 else col for i, col in enumerate(df.columns)]

# Step 6: Save the DataFrame to a JSON file
log_progress("Saving data to JSON file.")
json_file = "Countries_by_GDP.json"
df.to_json(json_file, orient="records", indent=4)

log_progress("Data successfully saved to JSON.")
print(f"Data saved to {json_file}.")

# Step 7: Save the DataFrame to a SQLite database
log_progress("Saving data to SQLite database.")
db_file = "World_Economies.db"
conn = sqlite3.connect(db_file)
df.to_sql("Countries_by_GDP", conn, if_exists="replace", index=False)
log_progress(f"Data successfully saved to table 'Countries_by_GDP' in database '{db_file}'.")
print(f"Data saved to table 'Countries_by_GDP' in database '{db_file}'.")

# Close the database connection
conn.close()

# Step 8: Query the database for entries with more than 100 billion USD economy
log_progress("Querying the database for economies greater than 100 billion USD.")

query = """
SELECT * 
FROM Countries_by_GDP
WHERE IMF > 1000000
"""

try:
    # Execute the query and load the results into a DataFrame
    conn = sqlite3.connect(db_file)
    result_df = pd.read_sql_query(query, conn)
    print("Countries with economies greater than 100 billion USD (IMF):")
    print(result_df)
    conn.close()
except Exception as e:
    log_progress(f"Query failed: {e}")
    print(f"An error occurred while querying the database: {e}")

# Step 9: Log the entire execution process
log_progress("Logging the entire process of execution.")

try:
    # Execute the adjusted query
    query = """
    SELECT * 
    FROM Countries_by_GDP
    WHERE IMF > 1000000
    """
    conn = sqlite3.connect(db_file)
    result_df = pd.read_sql_query(query, conn)
    log_progress("Query executed successfully. Data retrieved for economies greater than 100 billion (adjusted scale).")

    # Display and save the results
    print("Countries with economies greater than 100 billion USD (IMF):")
    print(result_df)
    result_df.to_csv("economies_above_100_billion_adjusted.csv", index=False)
    log_progress("Results saved to 'economies_above_100_billion_adjusted.csv'.")
except Exception as e:
    log_progress(f"An error occurred during query execution: {e}")
    print(f"An error occurred while querying the database: {e}")
finally:
    # Ensure the database connection is closed
    conn.close()
    log_progress("Database connection closed. Process complete.")

log_progress("Data extraction and processing complete.")
