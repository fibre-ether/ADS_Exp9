# i am not liable for any damage 
# use it at your own risk !!
# its against Etherscan's TOS 
# your ip might get ban for requesting too many times

import csv
import sys
import datetime
import requests
from time import sleep
from bs4 import BeautifulSoup
import lxml

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
}

def scraper(num_pages=1, req_delay=2):
  timestamp = datetime.datetime.now().strftime ("%Y%m%d_%H%M%S")
  
  print("%d pages to parse with delay of %f seconds between each page" % (num_pages, req_delay))
  print("before")
  api_url = "https://etherscan.io/contractsVerified/"
  print("after")

  response = requests.get(url=api_url)

  webpage = response.text
# print(webpage)

  
  with open('VerifiedContracts-'+timestamp+'.csv', 'w') as csvfile:
    fieldnames = ['addr', 'contract_name', 'compiler', 'balance', 'tx_count', 'date_verified']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for i in range(1, num_pages+1):
      url = api_url + str(i) + str('?ps=100')
      sleep(req_delay)
      response = requests.get(url, headers=headers)
      print("URL: %s, Status: %s" % (url, response.status_code))

      content = response.content
      # print(content)
      soup = BeautifulSoup(content, 'lxml')
      # print(soup.prettify())

      for row in soup.select('table.table-hover tbody tr'):
        cells = row.findAll('td')
        print(cells[0].text)
        cells = list(map(lambda x: x.text, cells))
        print(cells)
        # for i in range(len(cells)):
        #   print(cells[i], "\n\n")
        # for index, key in enumerate(cells):
        #   print(index, key)

        param = {'addr':0, 'contract_name':0, 'compiler':0, 'balance':0, 'tx_count':0, 'settings':0, 'date_verified':0}
        dt = ['addr', 'contract_name', 'compiler', 'balance', 'tx_count', 'settings', 'date_verified']

        for index, key in enumerate(param):
          print(index, key)
          param[key] = cells[index]
        print(param)

        # addr, contract_name, compiler, balance, tx_count, settings, date_verified = cells
        writer.writerow({
          'addr': param['addr'],
          'contract_name': param['contract_name'],
          'compiler': param['compiler'],
          'balance': param['balance'],
          'tx_count': param['tx_count'],
          'date_verified': param['date_verified'],
        })

def main():
  # if len(sys.argv) > 2:
  #   scraper(1, 2)
  # elif len(sys.argv) == 2:
  #   scraper(int(sys.argv[1]))
  # else:
  scraper()

if __name__ == "__main__":
  main()
