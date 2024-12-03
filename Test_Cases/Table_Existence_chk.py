import pandas as pd
import json
from datetime import datetime
from Utilities.Source_Target_DB_Conn import mysql_db_conn, oracle_db_conn
from Utilities.logging import Logs

source_db_conn = mysql_db_conn()
target_db_conn = oracle_db_conn()

# Calling Log_gen function from Utilities.
dt = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
logger = Logs.log_gen(f'C:\\Users\\MaNJu\\PycharmProjects\\ETL_Testing_Framework(Config Driven Framework)\\Logs\\Table_existence_{dt}.log')

with open(r'C:\Users\MaNJu\PycharmProjects\ETL_Testing_Framework(Config Driven Framework)\Config\SQL_Queries_Config\Tables_Checking.json', 'r') as Tables_chk_file:
     table_list = json.load(Tables_chk_file)

with open(r'C:\Users\MaNJu\PycharmProjects\ETL_Testing_Framework(Config Driven Framework)\Config\SQL_Queries_Config\Tables_Existance_Validations.json','r') as Data_Validation_file:
    validations = json.load(Data_Validation_file)

def Validate_Source_table_exist(db_conn):

    db_conn = source_db_conn
    SQL_query = validations['Table existence check']['Source_Query']
    tables_chk = table_list.get('Source_tables', [])
    results_list = []
    for table in tables_chk:
        query = SQL_query.format(table_name=table)

        try:
            result = pd.read_sql(query,db_conn)
            if result.empty:
                pass
            else:
                status_result = "table  exists"
        except Exception:
            status_result = "Table does not exists"

        results_list.append({"Table Name": table, "Status": status_result})
    source_dataframe = pd.DataFrame(results_list)
    return source_dataframe

#def Validate_target_table_exist(db_conn,SQL_query,tables_list):
def Validate_target_table_exist(db_conn):
    db_conn = target_db_conn
    SQL_query = validations["Table existence check"]["Target_Query"]
    tables_list = table_list.get('Target_tables',[])
    results_list = []

    for table in tables_list:
        query = SQL_query.format(table_name=table)

        try:
            result = pd.read_sql(query,db_conn)
            if result.empty or result.iloc[0,0]<=0:
                pass
            else:
                status_result ="table exists"
        except Exception :
            status_result = "Table does not exists"
        results_list.append({"Table Name":table,"Status":status_result})
    target_dataframe = pd.DataFrame(results_list)
    return target_dataframe

#Writing Output Result to excel file.
dt = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
output_path = f"C:\\Users\\MaNJu\\PycharmProjects\\ETL_Testing_Framework(Config Driven Framework)\\Output_Result\\Table_existence_validation_{dt}.xlsx"
with pd.ExcelWriter(output_path,engine='xlsxwriter') as writer:
    Validate_Source_table_exist(source_db_conn).to_excel(writer,sheet_name="Source Table Existence",index=False)
    Validate_target_table_exist(target_db_conn).to_excel(writer,sheet_name="Target Table Existence",index=False)

#logger.info(f"Table existence validation results have been written to {output_path}")
logger.info(f"Table existence validation results have been written to {output_path}")
