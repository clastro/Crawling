import requests
from bs4 import BeautifulSoup
from random import choice
import pandas as pd
import json

def proxy_generator(num):
    count = 0
    proxylist = pd.read_table('./proxylist.txt',header=None)
    if(num == 1):
        pass
    if(num == 2):
        count+=1
    yield {'https':proxylist[0][count]}
    
  def data_scraper(request_method, url, **kwargs):
    while True:
        try:
            proxy = proxy_generator(1)
            #print("Proxy currently being used: {}".format(proxy))
            response = requests.request(request_method, url, proxies=next(proxy), timeout=2, **kwargs)
            break
            # if the request is successful, no exception is raised
        except:
            print("Connection error, looking for another proxy")
            proxy = proxy_generator(2)
            next(proxy)
            pass
    return response
  
  
