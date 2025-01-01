# This code is part of the IBM Data Engineering Professional Certificate course on Coursera.
# © IBM Corporation. All rights reserved.

import sqlite3
import pandas as pd

# Establish a connection to SQLite database
def connect_to_db(db_name):
    return sqlite3.connect(db_name)

# Create a table from a Pandas DataFrame
def create_table_from_df(df, table_name, connection, if_exists='replace'):
    df.to_sql(table_name, connection, if_exists=if_exists, index=False)
    print(f"Table '{table_name}' created/updated successfully.")

# Query the database and return a DataFrame
def query_table(query, connection):
    return pd.read_sql(query, connection)

# Example usage
if __name__ == "__main__":
    # Connect to the database
    conn = connect_to_db("example.db")

    # Sample DataFrame
    sample_data = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Cyrus', 'Bob', 'Ario'],
        'age': [25, 14, 35]
    })

    # Create or update a table
    create_table_from_df(sample_data, "users", conn)

    # Query the table
    result_df = query_table("SELECT * FROM users", conn)
    print("Query Result:\n", result_df)

    # Close the connection
    conn.close()
