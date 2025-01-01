# This code is part of the IBM Data Engineering Professional Certificate course on Coursera.
# © IBM Corporation. All rights reserved.

# Coding starts...
# Extract, Transform, Load (ETL) Operations for Integrating Disparate Sources Including CSV, JSON, and XML Files
# Import required libraries
import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

# Global file paths
log_file = "log_file.txt"
target_file = "transformed_data.csv"

# Task 1. Extraction
## Function to Extract from CSV
def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe

## Function to Extract from JSON
def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process, lines=True)
    return dataframe

## Function to Extract from XML
def extract_from_xml(file_to_process):
    dataframe = pd.DataFrame(columns=["name", "height", "weight"])
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for person in root:
        name = person.find("name").text
        height = float(person.find("height").text)
        weight = float(person.find("weight").text)
        dataframe = pd.concat([dataframe, pd.DataFrame([{"name": name, "height": height, "weight": weight}])], ignore_index=True)
    return dataframe

## Function to Identify and Extract Data
def extract():
    extracted_data = pd.DataFrame(columns=["name", "height", "weight"])  # Create an empty dataframe

    # Process all CSV files
    for csvfile in glob.glob("*.csv"):
        extracted_data = pd.concat([extracted_data, extract_from_csv(csvfile)], ignore_index=True)

    # Process all JSON files
    for jsonfile in glob.glob("*.json"):
        extracted_data = pd.concat([extracted_data, extract_from_json(jsonfile)], ignore_index=True)

    # Process all XML files
    for xmlfile in glob.glob("*.xml"):
        extracted_data = pd.concat([extracted_data, extract_from_xml(xmlfile)], ignore_index=True)

    return extracted_data

# Task 2. Transformation

## Function to Transform and Convert Data into DEsired Format/Structure
def transform(data):
    '''Convert inches to meters and round off to two decimals'''
    data['height'] = round(data.height * 0.0254, 2)

    '''Convert pounds to kilograms and round off to two decimals'''
    data['weight'] = round(data.weight * 0.45359237, 2)

    return data

# Task 3. Loading and Logging
# Logging means recording events, messages, or data about the execution of the program...

# Function to load transformed data to a CSV file
def load_data(target_file, transformed_data):
    transformed_data.to_csv(target_file, index=False)

# Function to log progress with a timestamp
def log_progress(message):
    timestamp_format = "%Y-%m-%d-%H:%M:%S"  # Year-Month-Day Hour:Minute:Second
    now = datetime.now()  # Get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:  # Append log message to the log file
        f.write(timestamp + ', ' + message + '\n')


# Testing ETL Operations and Log Progress
# Log the initialization of the ETL process
log_progress("ETL Job Started")

# Log the beginning of the Extraction process
log_progress("Extract phase Started")
extracted_data = extract()

# Log the completion of the Extraction process
log_progress("Extract phase Ended")

# Log the beginning of the Transformation process
log_progress("Transform phase Started")
transformed_data = transform(extracted_data)
print("Transformed Data")
print(transformed_data)

# Log the completion of the Transformation process
log_progress("Transform phase Ended")

# Log the beginning of the Loading process
log_progress("Load phase Started")
load_data(target_file, transformed_data)

# Log the completion of the Loading process
log_progress("Load phase Ended")

# Log the completion of the ETL process
log_progress("ETL Job Ended")




