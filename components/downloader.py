"""This module has the functions needed for downloading datasets
 from https://datos.gob.ar CKAN API and save them into csv files"""

import requests
from decouple import config
from components.logs_config import log, log_settings
import components.dataframe_processor as proc
import sys
import datetime
import locale
import os
import pandas as pd

QUERY_STRING = config('QUERY_STRING')[1:-1]
API_URL = config('API_URL')[1:-1]
DATASETS = [
    {
        'name' : config('DATASET_1_NAME')[1:-1],
        'resource_id' : config('DATASET_1_ID')[1:-1]
        },
    {
        'name' : config('DATASET_2_NAME')[1:-1],
        'resource_id' : config('DATASET_2_ID')[1:-1]
        },
    {
        'name' : config('DATASET_3_NAME')[1:-1],
        'resource_id' : config('DATASET_3_ID')[1:-1]
        }
    ]

# Configuring loggings
log_settings()

def api_to_df(dataset:dict) -> pd.DataFrame:
    """Downloads a dataset from datos.gob.ar CKAN API 
    and saves it into a pandas dataframe

    Args:
        dataset (dict): Has to be a dictionary retrieved from
        DATASETS variable list from this module.
        
    Returns: 
        A pandas.DataFrame
    """
    try:
        url = API_URL + QUERY_STRING + dataset['resource_id']
        response = requests.get(url)
        response.raise_for_status()
        
        # access api json response
        data = response.json()['result']['records']
        columns_order = [item['id'] for item 
                         in response.json()['result']['fields']]
        
        # DataFrame creation
        name = dataset['name'].replace(' ', '_').lower()
        df = pd.DataFrame(data, columns=columns_order)
        log.info(f'{name} dataset downloaded')
        
        return df
    
    except:
        type, value, traceback = sys.exc_info()
        log.error(f'{type}, {value}')
        sys.exit(1)
        

def df_to_csv(df:pd.DataFrame, dataset:dict) -> None:
    """Saves a pandas dataframe into a csv in the following path
    (in argentinean spanish):
    
    /csv/categoría/año-mes/categoria-dia-mes-año.csv
    
    Args:
        df (pandas.DataFrame): Has to be a pandas dataframe
        returned from api_to_df() function of this module.
        dataset (dict): Required to retrieve dataset 
        category name to be uses as csv filename.
        Has to be a dictionary retrieved from 
        DATASET variable of this module. 
    """
    try:
        # Set spanish local formatting for time for folder naming  
        locale.setlocale(category=locale.LC_TIME, 
                         locale='Spanish_Argentina')
        _now = datetime.datetime.now()    #Current local time
        name = dataset['name'].replace(' ', '_').lower()
        date = f'{_now:%Y}-{_now:%B}'
        filename = f'{name}-{_now:%d}-{_now:%B}-{_now:%Y}.csv'
        
        # Get current directory
        base_path = os.getcwd()
        # Create csv folder if not exist
        csv_path = os.path.join(base_path, 'csv')
        
        if 'csv' not in os.listdir(base_path):
            os.mkdir('csv')
        # Change working directory
        os.chdir(csv_path)
            
        # Create category name folder if not exist
        if name not in os.listdir(csv_path):
            os.mkdir(name)
        # Change working directory
        category_path = os.path.join(csv_path, name)
        os.chdir(category_path)
            
        # Create date folder if not exist
        if date not in os.listdir(category_path):
            os.mkdir(date)
        # Change working directory
        os.chdir(os.path.join(category_path, date))

        # Save dataframe to csv    
        df.to_csv(filename, index = True)
        log.info(f'{name} dataset saved to csv')
        
        # Change working directory to base folder
        os.chdir(base_path)
    
    except:
        type, value, traceback = sys.exc_info()
        log.error(f'{type}, {value}')
        sys.exit(1)
        
def download_datasets() -> pd.DataFrame:
    """This function does the following steps:
    1. Downloads datasets from datos.gob.ar CKAN API
    2. Saves datasets into csv files
    3. Turns each dataset into a pandas dataframe
    4. Normalizes each pandas dataframe
    5. Concatenates each pandas dataframe 
    into one pandas dataframe

    Returns:
        A pandas.DataFrames
    """
    # An empty list to retrieve the datasets
    dataset_list = []

    for dataset in DATASETS: # iterate in DATASETS dictionaries list

        # Download dataset from api and save to pandas dataframe
        raw_df = api_to_df(dataset)
        
        # Save pandas dataframe to csv
        df_to_csv(raw_df, dataset)
        
        # Dataframe normalization
        norm_df = proc.normalize(raw_df, dataset)
        
        # Append df to dataset_list
        dataset_list.append(norm_df)
        
    final_df = proc.df_concat(dataset_list)
        
    return final_df

if __name__=='__main__':
    from logs_config import log, log_settings
    log_settings()
    df = api_to_df(DATASETS[0])
    print(df.head())