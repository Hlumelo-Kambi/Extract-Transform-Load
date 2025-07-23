import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

log_file = 'etl_log_file.txt'
target_file = 'transformed_data.csv'

def extract_from_csv(file_to_process):
    try:
        dataframe = pd.read_csv(file_to_process)
        return dataframe
    except Exception as e:
        print(f"Error reading {file_to_process}: {e}")
        return pd.DataFrame()

def extract_from_json(file_to_process):
    try:
        dataframe = pd.read_json(file_to_process, lines=True)
        return dataframe
    except Exception as e:
        print(f"Error reading {file_to_process}: {e}")
        return pd.DataFrame()

def extract_from_xml(file_to_process):
    data = []  # Collect data in a list first
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    
    for car in root:
        try:
            model_elem = car.find("model")
            year_elem = car.find("year_of_manufacture")
            price_elem = car.find("price")
            fuel_elem = car.find("fuel")
            
            if all([model_elem is not None, year_elem is not None, 
                    price_elem is not None, fuel_elem is not None]):
                data.append({
                    "car_model": model_elem.text,
                    "year_of_manufacture": int(year_elem.text),
                    "price": float(price_elem.text),
                    "fuel": fuel_elem.text
                })
        except (AttributeError, ValueError) as e:
            print(f"Skipping malformed XML entry: {e}")
            continue
    
    # Create DataFrame once at the end
    dataframe = pd.DataFrame(data)
    return dataframe

def extract(): 
    all_data = []  # Collect all DataFrames first
     
    # process all csv files, except the target file
    for csvfile in glob.glob("*.csv"): 
        if csvfile != target_file:  # check if the file is not the target file
            csv_data = extract_from_csv(csvfile)
            if not csv_data.empty:  # Only add non-empty DataFrames
                all_data.append(csv_data)
         
    # process all json files 
    for jsonfile in glob.glob("*.json"): 
        json_data = extract_from_json(jsonfile)
        if not json_data.empty:
            all_data.append(json_data)
     
    # process all xml files 
    for xmlfile in glob.glob("*.xml"): 
        xml_data = extract_from_xml(xmlfile)
        if not xml_data.empty:
            all_data.append(xml_data)
    
    # Concatenate all at once
    if all_data:
        return pd.concat(all_data, ignore_index=True)
    else:
        return pd.DataFrame(columns=["car_model","year_of_manufacture","price","fuel"])


def transform(data): 
    # Round price off to two decimals 
    data['price'] = round(data.price,2) 
     
    return data 

def load_data(target_file, transformed_data): 
    transformed_data.to_csv(target_file)

def log_progress(message): 
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open(log_file,"a") as f: 
        f.write(timestamp + ',' + message + '\n') 

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
load_data(target_file,transformed_data) 
  
# Log the completion of the Loading process 
log_progress("Load phase Ended") 
  
# Log the completion of the ETL process 
log_progress("ETL Job Ended") 