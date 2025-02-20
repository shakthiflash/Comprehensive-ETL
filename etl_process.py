import glob
import json
import logging
import os
from datetime import datetime
import pandas as pd
import xml.etree.ElementTree as ET

# Set up paths
log_file_path = r'C:\Users\sb\Desktop\Guvi_project\Logs\log_file.txt'
transformed_data_path = r'C:\Users\sb\Desktop\Guvi_project\Output\transformed_data.csv'

# Ensure the directory exists
os.makedirs(os.path.dirname(transformed_data_path), exist_ok=True)

os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

# Configure logging
logging.basicConfig(filename=log_file_path, level=logging.INFO)

def log(message):
    """Log a message with a timestamp."""
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

@log_execution_time
def extract_csv(file_path):
    """Extract data from a CSV file."""
    return pd.read_csv(file_path)

@log_execution_time
def extract_json(file_path):
    """Extract data from a JSON file."""
    data = []
    with open(file_path, 'r') as f:
        for line in f:
            data.append(json.loads(line))
    return pd.DataFrame(data)

@log_execution_time
def extract_xml(file_path):
    """Extract data from an XML file."""
    tree = ET.parse(file_path)
    root = tree.getroot()
    data = []
    for child in root:
        data.append(child.attrib)
    return pd.DataFrame(data)

@log_execution_time
def extract_data(file_path):
    """Extract data based on file type."""
    log(f'Data extraction started for {file_path}')
    if file_path.endswith('.csv'):
        return extract_csv(file_path)
    elif file_path.endswith('.json'):
        return extract_json(file_path)
    elif file_path.endswith('.xml'):
        return extract_xml(file_path)
    else:
        raise ValueError("Unsupported file format")

@log_execution_time
def combine_data(file_paths):
    """Combine data from multiple files into a single DataFrame."""
    combined_data = pd.DataFrame()
    for file_path in file_paths:
        data = extract_data(file_path)
        combined_data = pd.concat([combined_data, data], ignore_index=True)
    return combined_data

@log_execution_time
def transform_data(data):
    """Transform data by converting units."""
    if 'height' in data.columns:
        data['height_m'] = data['height'] * 0.0254
    if 'weight' in data.columns:
        data['weight_kg'] = data['weight'] * 0.453592
    return data

@log_execution_time
def load_data(data, file_path):
    """Load transformed data into a CSV file."""
    data.to_csv(file_path, index=False)

# ETL Execution
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
 