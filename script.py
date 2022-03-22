import os
import components.downloader as dw
import components.dataframe_processor as proc
import components.dbconnector as con
    
# Download datasets, save to csv and 
# concatenate them into one pandas dataframe
df1 = dw.download_datasets()

# Create a new postgresql database
con.create_database()

# Create 'sitios' table in db from pandas dataframe
con.df_to_dbtable(df1, table='sitios')

# Create 'totales' table in db from previous pandas dataframe
df2 = proc.totales(df1)
con.df_to_dbtable(df2, table='totales')

# Create 'cines' table in db from last saved csv file
raw_df3 = proc.csv_to_df(category='salas_de_cine')
df3 = proc.cines(raw_df3)
con.df_to_dbtable(df3, table='cines')

# ADITIONAL: database query with sql file
sqlfile_path = os.path.join(os.getcwd(),
                            'sql',
                            'select_from_where.sql')

query = con.sql_file_exec(path=sqlfile_path, 
                          table='cines', 
                          condition='butacas>10000'
                          )
print(query)
