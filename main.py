import pandas as pd
import glob
import requests
from datetime import datetime

tmpfile = 'temp.tmp'
logfile = 'logfile.txt'
targetfile = 'bank_market_cap_gbp.csv'

def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process)
    return dataframe

columns = ['Name', 'Market Cap (US$ Billion)']
def extract():
    extracted_data = pd.DataFrame(columns=columns)
    for jsonfile in glob.glob('*.json'):
        extracted_data = extracted_data.append(extract_from_json(jsonfile))
        return extracted_data

df = pd.read_csv('exchange_rates.csv', index_col=0)
exchange_rate = df.loc['GBP']

def transform(data):
    data['Market Cap (US$ Billion)'] = data['Market Cap (US$ Billion)'].transform(lambda x: x * exchange_rate).round(3)
    data = data.rename(columns={'Market Cap (US$ Billion)':'Market Cap (GBP$ Billion)'})
    return data

def load(targetfile, data_to_load):
    data_to_load.to_csv(targetfile)

def log(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open('logfile.txt', 'a') as f:
        f.write(timestamp + '' + message + '\n')

log('ETL Job Started \n')

log('Extract phase Started')
extracted_data = extract()
print(extracted_data.head())
log('Extract phase Ended \n')

log('Transform phase Started')
transfomred_data = transform(extracted_data)
print(transfomred_data.head())
log('Transform phase Ended \n')

log('Load phase Started')
load(targetfile, transfomred_data)
log('Load phase Ended \n')

log('ETL Job Ended \n')

    