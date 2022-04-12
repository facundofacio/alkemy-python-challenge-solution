"""This module has the functions needed for 
interacting with postgresql database 
and create all the required tables.
"""

import sys
import pandas as pd
from decouple import config
from components.logs_config import log, log_settings
import urllib.parse
from sqlalchemy import create_engine, text

DIALECT = config('DIALECT')
DRIVER = config('DRIVER')
USER = config('USER')
PASS = urllib.parse.quote_plus(config('PASS'))
DB_HOST = config('DB_HOST')
PORT = config('PORT')
DB_NAME = config('DB_NAME')
ENGINE_URL = f'{DIALECT}+{DRIVER}://{USER}:{PASS}@{DB_HOST}:{PORT}'
DB_URL = ENGINE_URL + '/' + DB_NAME

# Configuring loggings
log_settings()

def create_database():
    """Creates a new database in postgresql.
    The database name is retreived from settings.ini file
    """
    try:
        # Checks if database already exists
        with create_engine(ENGINE_URL,
                        isolation_level='AUTOCOMMIT'
                        ).connect() as conn:
            file = open('sql\\db_exists.sql')
            query = file.read().format(
                db_name=DB_NAME)
            file.close()
            exists = conn.execute(query).fetchone()
            if exists:
                log.warning(f'{DB_NAME} database already exists')
            # Create database if it does not exist
            else:
                file = open('sql\\create_db.sql')
                query = file.read().format(
                    db_name=DB_NAME)
                file.close()
                conn.execute(query)
                log.info(f'{DB_NAME} database created')
    except:
        type, value, traceback = sys.exc_info()
        log.error(f'{type}, {value}')
        sys.exit(1)

def df_to_dbtable(df:pd.DataFrame, table:str='sitios'):
    """Creates a new table in database from a pandas dataframe.
    If exists previously it gets replaced.
    
    Args:
        df (pandas.DataFrame)
        table (str): The name of the new table (Default: sitios)
        """
    try:
        with create_engine(DB_URL, 
                        isolation_level='AUTOCOMMIT'
                        ).connect() as conn:
            
            # Drop 'fuente' column if table=sitios
            if table=='sitios':
                df = df.drop(columns='fuente')
            
            # pandas df to new db table
            df.to_sql(table, con=conn, if_exists='replace')
            
            # Set index as primary key
            file = open('sql\\add_primary_key.sql')
            query = file.read().format(
                tab=table)
            file.close()
            conn.execute(text(query))
            
            # Add date column
            file = open('sql\\add_date_column.sql')
            query = file.read().format(
                tab=table, 
                col='fecha_de_carga')
            conn.execute(text(query))
            file.close()
            log.info(f'{table} table added in {DB_NAME} database')
    except:
        type, value, traceback = sys.exc_info()
        log.error(f'{type}, {value}')
        sys.exit(1)           

def sql_file_exec(path:str,**params) -> pd.DataFrame:
    """Executes any SQL script file.
    If there is a query result, 
    it is returned as a pandas dataframe.
    
    Args:
        path (str): Path of the sql script file.
        **params: if the script file has {params},
        they can be passed as consequent arguments.
        
        IMPORTANT: params in .sql file need to be 
        surrounded by curly braces {}
        
    Result: 
        A pandas.DataFrame
    
    Use example:
    
    sql_file_exec('script.sql', table=users, col=name)
    """
    try:
        with create_engine(DB_URL, 
                        isolation_level='AUTOCOMMIT'
                        ).connect() as conn:
            file = open(path)
            if params:
                query = file.read().format(**params)
            else:
                query = file.read()
            file.close()
            result = conn.execute(text(query))
            if result:
                df = pd.read_sql_query(sql=query, con=conn)
                msg = 'Query executed and saved into pandas dataframe'
                log.info(msg)
                log.info(query.center(20))
                return df
            else:
                log.info(f'Query executed')
                log.info(query.center(20))
                
    except:
        type, value, traceback = sys.exc_info()
        log.error(f'{type}, {value}')
        sys.exit(1)

if __name__=='__main__':
    create_database()
    create_database()
    
    with create_engine(
        ENGINE_URL,
        isolation_level='AUTOCOMMIT'
        ).connect() as conn:
    
        conn.execute(f'drop database {DB_NAME}')
        log.info(f'{DB_NAME} database deleted')
