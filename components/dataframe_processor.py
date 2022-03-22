"""This module has functions needed for normalizing the downloaded
datasets and for concatenating them into one pandas dataframe"""

from components.logs_config import log, log_settings
import sys
import os
import pandas as pd
import numpy as np

# Target Column names
DB_COLUMN_NAMES = ['cod_localidad', 'id_provincia', 'id_departamento', 
                   'categoria', 'provincia', 'localidad', 
                   'nombre', 'domicilio', 'codigo_postal', 
                   'numero_de_telefono', 'mail', 'web', 'fuente'
                   ]

# Configuring loggings
log_settings()

def normalize(df:pd.DataFrame, 
              dataset:dict, 
              drop_old_cols:bool=True
              ) -> pd.DataFrame:
    """Normalizes dataset columns for future concatenation.
    dataset argument is needed to retrieve dataset category name.
    
    Args:
        df (pandas.DataFrame). Has to be a dataframe created
        with api_to_df() function of downloader module.
        dataset (dict): Has to be a dictionary retrieved from
        DATASET variable of downloader module. Needed to retrieve
        dataset category name.
        drop_old_cols (bool): If False it keeps original columns.
        Defaults to True
    
    Returns: 
        A pandas.DataFrame"""
    try:
        # Transform column names to lowercase
        df.columns = map(str.lower, df.columns) 
        
        # Insert 'categoría' column
        df['categoria'] = dataset['name']
        
        if dataset['name'] == 'Museos':
            # localidad_id refactorization
            df['id_departamento'] = df['localidad_id'].apply(
                lambda x: str(x)[:-3]).astype('int64')
            
            # Rename columns
            df.rename(columns = {'localidad_id':'cod_localidad', 
                                'provincia_id':'id_provincia', 
                                'id_departamento':'id_departamento', 
                                'direccion':'domicilio', 
                                'codigo_postal':'codigo_postal', 
                                'telefono':'numero_de_telefono'
                                },
                    inplace = True)
        
        elif dataset['name'] == 'Salas de cine':
                        
            # Rename columns
            df.rename(columns = {'cod_loc':'cod_localidad', 
                                'idprovincia':'id_provincia', 
                                'iddepartamento':'id_departamento', 
                                'direccion':'domicilio', 
                                'cp':'codigo_postal', 
                                'teléfono':'numero_de_telefono',
                                'Fuente':'fuente'
                                },
                    inplace = True)
            
        elif dataset['name'] == 'Bibliotecas populares':
            
            # Rename columns
            df.rename(columns = {'cod_loc':'cod_localidad', 
                                'idprovincia':'id_provincia', 
                                'iddepartamento':'id_departamento', 
                                'direccion':'domicilio', 
                                'cp':'codigo_postal', 
                                'teléfono':'numero_de_telefono',
                                'Fuente':'fuente'
                                },
                    inplace = True)
            
        # Subset and reorder columns
        if drop_old_cols==True:
            df = df.reindex(columns=DB_COLUMN_NAMES)
            
        # null values refactorization
        to_replace = ['s/d', '',' ', '"']
        final_df = df.replace(to_replace=to_replace, value=np.nan)
        
        # numero_de_telefono refactorization
        final_df.numero_de_telefono = final_df.numero_de_telefono.astype('float64')
        
        name = dataset['name'].replace(' ', '_').lower()
        log.info(f'{name} dataset normalized')
        
        return final_df
    
    except:
        type, value, traceback = sys.exc_info()
        log.error(f'{type}, {value}')
        sys.exit(1)


def df_concat(df_list:list # of pandas dataframes
              ) -> pd.DataFrame:
    """Concatenates a list of 
    pandas dataframes into one dataframe
    
    Args:
        df_list (a list of pandas.DataFrames)
    
    Returns: 
        A pandas.DataFrame
    """
    try:
        df = pd.concat(df_list, axis=0, ignore_index = True)
        
        log.info('All datasets combined into one pandas dataframe')
        
        return df
    except:
        type, value, traceback = sys.exc_info()
        log.error(f'{type}, {value}')
        sys.exit(1)
        
def totales(df:pd.DataFrame) -> pd.DataFrame:
    """Creates a new dataframe with unique value 
    counts of the following columns:
        -'categoria'\n
        -'fuente'\n
        -'provincia' and 'categoria' concatenated
    
    Args:
        df (pandas.DataFrame): Has to be a 
        pandas dataframe generated from 
        download_datasets() 
        function of downloader module.
    
    Returns:
        A pandas.DataFrame
    """
    try:
        # Categoría value counts
        categoria = df.categoria.value_counts()

        # Fuente value counts
        fuente = df.fuente.value_counts()
        
        # Provincia and categoría value counts
        cols = ['provincia', 'categoria']  # column selection
        df.sort_values(by=cols, inplace=True) # sorting
        df['prov_cat'] = df[cols].apply( #new combined column
            lambda row: ' - '.join(
                row.values.astype(str)
                ), axis=1
            )
        prov_cat = df.prov_cat.value_counts(sort=False)
        
        # A dictionary with value counts dataframes to iterate
        data = {'categoria': categoria,
                'fuente': fuente,
                'provincia_categoria': prov_cat}
        
        # An empty list to retrieve the dataframes
        df_list = []
        
        # New columns for final dataframe
        new_cols = ['columna', 'valor', 'registros_totales']
        
        # dataframe iteration processing
        for key, val in data.items():
            temp_df = pd.DataFrame(val)
            temp_df.reset_index(inplace=True)
            temp_df.columns = [new_cols[1], new_cols[2]]
            temp_df[new_cols[0]] = key
            temp_df = temp_df.reindex(columns=new_cols)
            df_list.append(temp_df)
        
        # Final dataframe concatenation
        final_df = pd.concat(df_list, axis=0, ignore_index = True)
        
        log.info('totales dataframe created')
        
        return final_df
        
    except:
        typ, value, traceback = sys.exc_info()
        log.error(f'{typ}, {value}')
        sys.exit(1)
        
def csv_to_df(category:str='salas_de_cine') -> pd.DataFrame:
    """Looks for the last saved csv of selected category
    and converts it to a pandas dataframe

    Args:
        category (str, optional): Category of data look for csv file.
        {'bibliotecas_populares', 'museos', 'salas_de_cine'}
        Defaults to 'salas_de_cine'.

    Returns:
        A pandas.DataFrame
    """
    try:
        # The path for searching the csv file
        path = os.path.join(os.getcwd(),
                            'csv',
                            category)
        
        # An empty list to retrieve csv file paths
        list_of_files = []
        
        # Loop for retrieve the list of csv file paths
        for (root, dirs, files) in os.walk(path, topdown=True):
            for name in files:
                filepath = (os.path.join(root, name))
                if filepath[-4:] == '.csv':
                    list_of_files.append(filepath)
        
        # Retrieve the last csv file path
        latest_file = max(list_of_files, key=os.path.getctime)
        
        # Convert csv file to a pandas dataframe
        df = pd.read_csv(latest_file)
        
        log.info(f'Last {category} csv file saved to pandas dataframe')
        
        return df
        
    except:
        typ, value, traceback = sys.exc_info()
        log.error(f'{typ}, {value}')
        sys.exit(1)
        
def cines(df:pd.DataFrame) -> pd.DataFrame:
    """Creates a new dataframe with the sum of the
    following columns, grouped by 'provincia' column:
        -'pantallas'\n
        -'butacas'\n
        -'espacio_incaa'

    Args:
        df (pandas.DataFrame): Has to be a pandas dataframe
        generated from csv_to_df() function of this module, 
        with category argument set to 'salas_de_cine' (default).

    Returns:
        A pandas.DataFrame
    """
    try:
        # List of columns to retrieve for further processing 
        cols = ['provincia', 'pantallas', 'butacas', 'espacio_incaa']
        
        # Transform column names to lowercase
        df.columns = map(str.lower, df.columns)
        temp_df = df[cols]
        
        # null values refactorization
        to_replace = ['s/d', '',' ', '"', '0']
        temp_df = temp_df.replace(to_replace=to_replace, value=np.nan)
        
        # Lower espacio_incaa values
        temp_df[cols[3]] = temp_df[cols[3]].apply(
            lambda x: x.lower() if x is str else x
            )
        
        # Sum pantallas and butacas grouped by provincia
        final_df = temp_df.groupby(
            [cols[0]]
            )[cols[1:3]].apply(
                lambda x : x.astype(int).sum())
        
        # Count espacios_incaa grouped by provincia
        final_df[cols[3]] = temp_df.groupby(
            [cols[0]]
            )[cols[3]].apply(
                lambda x : x.count())
        
        # Reset dataframe index
        final_df.reset_index(inplace=True)
        
        log.info('cines dataframe created')
        
        return final_df
    
    except:
        typ, value, traceback = sys.exc_info()
        log.error(f'{typ}, {value}')
        sys.exit(1)
