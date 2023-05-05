import requests
from datetime import datetime, timedelta
import pandas as pd
import time

def get_time():
  now = datetime.now()
  # For getting transactions per minute need to change this to (minutes=1)
  start_time = now - timedelta(days=1)
  start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S")

  end_time_str = now.strftime("%Y-%m-%d %H:%M:%S")
  return start_time_str, end_time_str

def scraper(type):
  start, end = get_time()
  if type=='Etherium':
    url = f"https://api.blockchair.com/ethereum/blocks?q=time({start}..{end})&limit=100"
    response = requests.get(url)
  elif type=='Bitcoin':
    url = "https://api.blockchair.com/bitcoin/transactions"
    limit = 100
    offset = 0
    page = 0
    params1 = {
        "s": "output_total(desc)",
        "q": f"time({start}..{end})",
        "limit": limit,
        "offset": offset,
        "page": page
    }
    response = requests.get(url, params=params1)
  else:
     print("Invalid Type")
  if response.status_code == 200:
      data = response.json()['data']
  else:
      print("Error: ", response.status_code)
  # a dataframe with 100 rows of transactions will get created
  df = pd.DataFrame(data)
  return df

def main():
   while(True):
      df = scraper('Bitcoin')
      df.to_csv("bitcoin.csv",index=False)
      break
      time.sleep(60)

main()