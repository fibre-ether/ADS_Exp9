import asyncio
from websockets.server import serve
import pandas as pd
import sqlite3
import pickle
from constants import *


conn = sqlite3.connect('analysis.sqlite')
sql_query = f'SELECT * FROM {table_name}'

async def echo(websocket):
    query = ""
    async for message in websocket:
        if message:
            print("query received: ", message)
            query = message
            break
        else:
            print("No query received")
            break
    
    pickled_df = None
    try:
        # SELECT * FROM {table_name} WHERE Age='12 secs ago'
        parsed_query = pd.read_sql_query(query if query else sql_query, conn)
        df = pd.DataFrame(parsed_query, columns=columns)
        print(df.shape)
        # df = pd.read_csv("output.csv") 
        pickled_df = pickle.dumps(df)
    except Exception as e:
        print("Exception", e)
    finally:
        await websocket.send(pickled_df)


async def main():
    async with serve(echo, "localhost", 8765):
        await asyncio.Future()

asyncio.run(main())
