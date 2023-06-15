"""
read a json file
then flatten the data
and then save data to database

Tech_used: JSON, PANDAS, POSTGRESQL, SQLALCHEMY

"""


import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import time

start_time=time.time()
DB="JSONDATA"
HOST='localhost'
PORT=5432
PASSWORD=#
DBURL='postgresql://postgres:{}@localhost:{}/{}'.format(quote_plus(PASSWORD),PORT,DB)
TABLE_NAME="COMPLEX_JSON_TO_DF_TO_DB"


print("-"*50)
df=pd.read_json("./complex_data.json")
print("data read successfully...")
print("-"*50)
print("flattening json data....")
flattened_data_df=pd.json_normalize(df.to_dict(orient="records"))
print("flattened successfully....")
print("flattened_df:\n",flattened_data_df.head(5))
print("-"*50)

print("write the data to a database table")

engine=create_engine(DBURL)
print("Engine created...")
connection=engine.connect()
try:
    if connection:
        print('connetion established...')
        flattened_data_df.to_sql(name=TABLE_NAME,con=connection,if_exists='replace',index=False)
        print(f"Data written successfully to {DB}.{TABLE_NAME}")
        print("-"*50)
    time_taken=time.time() -start_time
    print(f"Time taken by entire process : {time_taken} s")

    print("\n","-"*50)
except Exception as e:
    print(str(e))
    print("-"*50)