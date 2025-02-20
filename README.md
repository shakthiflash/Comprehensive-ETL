Comprehensive ETL Project
This project demonstrates a comprehensive ETL (Extract, Transform, Load) process using Python. The script extracts data from multiple file formats (CSV, JSON, XML), transforms the data, and loads it into a CSV file. The process is logged for monitoring and debugging purposes.

Prerequisites
Python 3.10 or higher
Required Python packages: pandas, glob, json, logging, os, datetime, xml.etree.ElementTree
Installation
Clone the repository:

git clone https://github.com/shakthiflash/Comprehensive-ETL.git
cd Comprehensive-ETL
Install the required packages:

pip install pandas
Directory Structure
Comprehensive-ETL/
│
├── etl_process.py
├── Logs/
│   └── log_file.txt
├── Output/
│   └── transformed_data.csv
└── Source/
    ├── data1.csv
    ├── data2.json
    └── data3.xml
Usage
Set up paths:

log_file_path: Path to the log file.
transformed_data_path: Path to the output CSV file.
Ensure directories exist:

The script will create the necessary directories if they do not exist.
Configure logging:

Logs are saved to the specified log file.
Run the ETL process:

python etl_process.py
Script Overview
Imports
import glob
import json
import logging
import os
from datetime import datetime
import pandas as pd
import xml.etree.ElementTree as ET
Set up paths
log_file_path = r'C:\Users\sb\Desktop\Guvi_project\Logs\log_file.txt'
transformed_data_path = r'C:\Users\sb\Desktop\Guvi_project\Output\transformed_data.csv'
Ensure directories exist
os.makedirs(os.path.dirname(transformed_data_path), exist_ok=True)
os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
Configure logging
logging.basicConfig(filename=log_file_path, level=logging.INFO)
Logging functions
def log(message):
    logging.info(f'{datetime.now()}: {message}')

def log_execution_time(func):
    def wrapper(*args, **kwargs):
        start_time = datetime.now()
        log(f'Starting {func.__name__}')
        result = func(*args, **kwargs)
        end_time = datetime.now()
        log(f'Completed {func.__name__} in {end_time - start_time}')
        return result
    return wrapper
Extraction functions
@log_execution_time
def extract_csv(file_path):
    return pd.read_csv(file_path)

@log_execution_time
def extract_json(file_path):
    data = []
    with open(file_path, 'r') as f:
        for line in f:
            data.append(json.loads(line))
    return pd.DataFrame(data)

@log_execution_time
def extract_xml(file_path):
    tree = ET.parse(file_path)
    root = tree.getroot()
    data = []
    for child in root:
        data.append(child.attrib)
    return pd.DataFrame(data)
Extract data based on file type
@log_execution_time
def extract_data(file_path):
    log(f'Data extraction started for {file_path}')
    if file_path.endswith('.csv'):
        return extract_csv(file_path)
    elif file_path.endswith('.json'):
        return extract_json(file_path)
    elif file_path.endswith('.xml'):
        return extract_xml(file_path)
    else:
        raise ValueError("Unsupported file format")
Combine data from multiple files
@log_execution_time
def combine_data(file_paths):
    combined_data = pd.DataFrame()
    for file_path in file_paths:
        data = extract_data(file_path)
        combined_data = pd.concat([combined_data, data], ignore_index=True)
    return combined_data
Transform data
@log_execution_time
def transform_data(data):
    if 'height' in data.columns:
        data['height_m'] = data['height'] * 0.0254
    if 'weight' in data.columns:
        data['weight_kg'] = data['weight'] * 0.453592
    return data
Load data into a CSV file
@log_execution_time
def load_data(data, file_path):
    data.to_csv(file_path, index=False)
ETL Execution
log('ETL process started')

# Extraction Phase
log('Data extraction started')
file_paths = glob.glob(r'C:\Users\sb\Desktop\Guvi_project\Source\*')
extracted_data = combine_data(file_paths)
log('Data extraction completed')

# Inspect the extracted data
print(extracted_data.head())

# Transformation Phase
log('Data transformation started')
transformed_data = transform_data(extracted_data)
log('Data transformation completed')

# Loading Phase
log('Data loading started')
load_data(transformed_data, transformed_data_path)
log('Data loading completed')

log('ETL process completed')
Conclusion
This ETL script extracts data from various file formats, transforms the data by converting units, and loads the transformed data into a CSV file. The process is logged for monitoring and debugging purposes.
