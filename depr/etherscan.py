from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver import Chrome

import csv
import datetime
import time
import pandas as pd
import os

driver = webdriver.Chrome()




# pages = 2
def scrape(pages=2):
    rows = []
    fieldnames= ["1","Txn Hash", "Method", "Block","2", "Age","3", "From","4", "To", "Value", "Txn Fee","5"]
    for i in range(1, pages):
        driver.get(f'https://etherscan.io/txs?p={i}')
        time.sleep(5)
        try:
            table = driver.find_element(By.XPATH, """//*[@id="ContentPlaceHolder1_divTransactions"]/div[2]/table""")

            for tr in table.find_element(By.XPATH, './tbody').find_elements(By.TAG_NAME, 'tr'):
                row = [td.text.strip() for td in tr.find_elements(By.TAG_NAME, 'td')]
                rows.append(row)
            print(f"{i} Iteration Completed")
        except Exception as e:
            print(e)
        if(i%20==0):
            df = pd.DataFrame(rows, columns = fieldnames)
            print(df.head())
            timestamp = datetime.datetime.now().strftime ("%Y%m%d_%H%M%S")
            df.to_csv(f"transactions-{timestamp}-{i}.csv")
            rows = []
    df = pd.DataFrame(rows, columns = fieldnames)
    print(df.head())
    # timestamp = datetime.datetime.now().strftime ("%Y%m%d_%H%M%S")

    # data = 'data'
    # path = os.path.join(data, f"transactions-{timestamp}-{i}.csv")

    # df.to_csv(path)
    print("SCRAPING DONE")
    driver.quit()
    return df

