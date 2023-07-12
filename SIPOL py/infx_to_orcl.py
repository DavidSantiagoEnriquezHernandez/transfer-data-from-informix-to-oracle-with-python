import pandas as pd
import cx_Oracle
import pyodbc
import warnings
warnings.filterwarnings('ignore')

cx_Oracle.init_oracle_client(lib_dir=r"C:\oracle\instantclient_19_19")

# Conn with infx
conn_informix = pyodbc.connect("DRIVER=IBM INFORMIX ODBC DRIVER (64-bit);"
                               #your connection 
                               
                               )

# initial query to obtain the data that is going to be moved to orcl db
query_informix = "SELECT foto_idpersona, foto_cvetipofoto, foto_descripcion, foto_imagen FROM sipol_viw_fotos"

# Variables for filtering data with batches
batch_size = 500  
start_id = 282720 

while True:
    # query to obtain the data according to the batch size and start id
    query_batch = f"{query_informix} WHERE foto_idpersona >= {start_id} AND foto_idpersona < {start_id + batch_size}"
    df_informix = pd.read_sql_query(query_batch, conn_informix)
    print(df_informix)

    # break the loop if theres not more data found
    if df_informix.empty:
        break

    # Conn with orcl
    oracle_conn = cx_Oracle.connect(
      #your connection
    )

    # query to obtain the data to be compared 
    query_oracle = "SELECT FOTO_IDPERSONA FROM FOTOS_SIPOL_PERSONAS WHERE FUENTE_ORIGEN = 'CUAUHTEMOC'"
    df_oracle = pd.read_sql_query(query_oracle, oracle_conn)

    # Find the missing data in the actual batch
    registros_faltantes = df_informix[~df_informix['foto_idpersona'].isin(df_oracle['FOTO_IDPERSONA'])]
    print(registros_faltantes)

    # Insert the missing data in oracle
    if not registros_faltantes.empty:
        # Make the insertion in smaller batches
        batch_insert = registros_faltantes.to_dict(orient='records')
        oracle_cursor = oracle_conn.cursor()
        consulta_insercion = "INSERT INTO FOTOS_SIPOL_PERSONAS (FOTO_IDPERSONA, FOTO_CVETIPOFOTO, FOTO_DESCRIPCION, FOTO_IMAGEN, FUENTE_ORIGEN) VALUES (:FOTO_IDPERSONA, :FOTO_CVETIPOFOTO, :FOTO_DESCRIPCION, :FOTO_IMAGEN, 'CUAUHTEMOC')"
        oracle_cursor.executemany(consulta_insercion, batch_insert)
        oracle_conn.commit()
        oracle_cursor.close()
        print("Registros insertados en Oracle")

    # Close the connection
    oracle_conn.close()

    # increase the initial value for the next batch
    start_id += batch_size

# Close the conn with informix
conn_informix.close()

