from datetime import datetime
import pandas as pd
import requests
from bs4 import BeautifulSoup
from io import StringIO
import numpy as np
import sqlite3

pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.width', 1000)       # Increase width for better visibility

# Task 1: Logging function
def log_progress(message):
    """
    Logs the progress of the code at different stages in a file.
    The log format is: <time_stamp> : <message>
    """
    log_file = "code_log.txt"  # Name of the log file
    with open(log_file, "a") as file:
        time_stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        file.write(f"{time_stamp} : {message}\n")

# Task 2: Data extraction function
def extract(url, table_attribs):
    """
    Extracts tabular data from the given URL based on table attributes.
    Returns the extracted data as a pandas DataFrame.
    """
    log_progress("Starting data extraction process.")

    # Send GET request to the URL
    response = requests.get(url)
    if response.status_code != 200:
        log_progress("Failed to fetch the webpage.")
        raise Exception("Failed to retrieve the URL content.")

    soup = BeautifulSoup(response.content, "html.parser")

    # Locate the table based on attributes
    table = soup.find("table", table_attribs)
    if table is None:
        log_progress("No table found with the specified attributes.")
        raise ValueError("Table not found on the webpage.")

    # Extract table data into a DataFrame using StringIO to avoid deprecation warnings
    df = pd.read_html(StringIO(str(table)))[0]
    log_progress("Data extraction complete. Returning DataFrame.")
    return df

# Task 3: Transformation function
def transform(df, csv_path):
    """
    Transforms the DataFrame by adding new columns for market cap in GBP, EUR, and INR.
    Reads the exchange rates from a CSV file and applies the rates.
    """
    log_progress("Starting data transformation process.")

    # Read the exchange rate CSV file into a dictionary
    exchange_rates = pd.read_csv(csv_path, index_col=0).squeeze("columns").to_dict()

    # Add new columns for market cap in GBP, EUR, and INR
    df['MC_GBP_Billion'] = [np.round(x * exchange_rates['GBP'], 2) for x in df['Market cap (US$ billion)']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rates['EUR'], 2) for x in df['Market cap (US$ billion)']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rates['INR'], 2) for x in df['Market cap (US$ billion)']]

    log_progress("Data transformation complete. Returning transformed DataFrame.")
    return df

# Task 4: Loading to CSV function
def load_to_csv(df, output_path):
    """
    Saves the transformed DataFrame to a CSV file.
    """
    log_progress("Starting loading process to CSV.")
    df.to_csv(output_path, index=False)
    log_progress(f"Data successfully saved to {output_path}.")

# Task 5: Loading to Database function
def load_to_db(df, sql_connection, table_name):
    """
    Saves the transformed DataFrame to a SQLite database table.
    """
    log_progress("Starting loading process to database.")
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
    log_progress(f"Data successfully loaded to table '{table_name}' in the database.")

# Task 6: Running Queries on Database
def run_queries(sql_connection):
    """
    Executes and displays the results of specific queries on the database.
    """
    log_progress("Starting query execution.")

    # Query 1: Print the contents of the entire table
    query1 = "SELECT * FROM Largest_banks"
    print("Query 1 - Entire Table:")
    print(pd.read_sql_query(query1, sql_connection))

    # Query 2: Print the average market capitalization in GBP
    query2 = "SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
    print("\nQuery 2 - Average Market Cap (GBP):")
    print(pd.read_sql_query(query2, sql_connection))

    # Query 3: Print the names of the top 5 banks
    query3 = "SELECT [Bank name] FROM Largest_banks LIMIT 5"
    print("\nQuery 3 - Top 5 Banks:")
    print(pd.read_sql_query(query3, sql_connection))

    log_progress("Query execution complete.")

# Main code execution
if __name__ == "__main__":
    url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
    table_attribs = {"class": "wikitable"}
    csv_path = "exchange_rate.csv"  # Path to the exchange rate CSV file
    output_path = "transformed_data.csv"  # Output CSV file path
    db_path = "Banks.db"  # SQLite database path
    table_name = "Largest_banks"  # Table name in the database

    try:
        # Task 2: Extract data
        df = extract(url, table_attribs)
        log_progress("Data extraction successful.")

        # Task 3: Transform data
        df = transform(df, csv_path)
        log_progress("Data transformation successful.")

        # Task 4: Load to CSV
        load_to_csv(df, output_path)
        log_progress("CSV file created successfully.")

        # Task 5: Load to Database
        with sqlite3.connect(db_path) as conn:
            load_to_db(df, conn, table_name)

            # Task 6: Run Queries
            run_queries(conn)

        log_progress("Database loading and query execution successful.")
    except Exception as e:
        log_progress(f"Error occurred: {e}")
        print(f"An error occurred: {e}")
