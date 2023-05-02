import asyncio
from websockets.server import serve
import pandas as pd
import pickle


async def echo(websocket):
    pickled_df = None
    try:
        df = pd.read_csv("output.csv")
        pickled_df = pickle.dumps(df)
    finally:
        await websocket.send(pickled_df)


async def main():
    async with serve(echo, "localhost", 8765):
        await asyncio.Future()

asyncio.run(main())
