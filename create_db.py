
import pandas as pd
import sqlite3
from constants import *
from colab import analysis_iteration

# def create_db():
#     df = pd.read_csv('data/temp_data.csv')
#     df = df.loc[:,df.notna().all(axis=0)].iloc[:,1:]
#     print(df.head())


#     conn = sqlite3.connect('analysis.sqlite')
#     query = f'Create table if not Exists {table_name} (TxnHash text ,Method text ,Block integer ,Age integer ,FromAddr text ,ToAddr text ,Value text ,TxnFee real)'
#     conn.execute(query)
#     df.to_sql(table_name,conn,if_exists='replace',index=False)
#     conn.commit()
#     conn.close()
