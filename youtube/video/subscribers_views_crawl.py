import pandas as pd
from requests_html import HTMLSession,AsyncHTMLSession 
from bs4 import BeautifulSoup as bs # importing BeautifulSoup
from tqdm import tqdm
import nest_asyncio
import json
import re
import numpy as np
import pymysql
import time
from datetime import date
from sqlalchemy import create_engine

host="host address"
port="your port number"
user="user name"
passwd="your password"
db="your db"

conn  = pymysql.connect(host=host,port=port,user=user,passwd=passwd,db=db)  #데이터베이스 연결
cursor = conn.cursor()
conn.set_charset('utf8')

sql = """
    SELECT channel_id FROM youtuber_info
"""

cursor.execute(sql)
df_channel = pd.DataFrame(cursor.fetchall(),columns=[item[0] for item in cursor.description])

cnt = 0
channel_results = {}
for channel_id in tqdm(df_channel['channel_id']):
    if(cnt % 9 == 2):
        time.sleep(3)
    cnt += 1
    result = {}
    video_url = "https://www.youtube.com/channel/{}/about".format(channel_id)
    session = HTMLSession()
    response = session.get(video_url)
    if(response.status_code == 429):
        print(response)
    soup = bs(response.html.html, "html.parser")
    data = re.search(r"var ytInitialData = ({.*?});", soup.find_all("script")[33].prettify()).group(1)
    data_json = json.loads(data)
    
    if 'alerts' in data_json:
        if 'ERROR' in data_json['alerts'][0]['alertRenderer']['type']: #계정 해지 or 신고
            continue
            
    string_contain_views = str(pd.DataFrame(data_json)['contents']['twoColumnBrowseResultsRenderer']['tabs'])
    
    if '조회수' in string_contain_views:
        string_num = string_contain_views.index('조회수')
        numbers = re.sub(r'[^0-9]', '', string_contain_views[string_num+3:string_num+25])
        total_views = int(numbers)
    else:
        total_views = ''    
    
    if 'subscriberCountText' in pd.DataFrame(data_json)['header']['c4TabbedHeaderRenderer']:
        raw_subscribers = pd.DataFrame(data_json)['header']['c4TabbedHeaderRenderer']['subscriberCountText']['simpleText']
        raw_subscribers = raw_subscribers.replace("억","_100000000")
        raw_subscribers = raw_subscribers.replace("만","_10000")
        raw_subscribers = raw_subscribers.replace("천","_1000")
        raw_subscribers = raw_subscribers.replace("백","_100")
        raw_subscribers = raw_subscribers.replace("십","_10")
        subscribers = re.findall(r'\d+', raw_subscribers)
        unit = int(subscribers[-1])
        if(len(subscribers) == 1): #단위가 존재하지 않을 경우 (백,천,만)
            total_subscribers = int(subscribers[0])
        elif(len(subscribers) ==2): #단위가 존재할 경우 (30만, 2만)
            total_subscribers = int(subscribers[0]) * unit
        else: #소수점과 단위가 함께 존재할 경우 (3.2만, 2.5억)
            total_subscribers = int(subscribers[0]) * unit + int(subscribers[1]) * unit/100
        total_subscribers = int(total_subscribers)
    else:
        total_subscribers = ''         
    
    result['views'] = total_views
    result['subscribers'] = total_subscribers
    result['date'] = date.today().isoformat()
    channel_results[channel_id] = result

insert_df = pd.DataFrame(channel_results).T
insert_df['channel_id'] = insert_df.index

pymysql.install_as_MySQLdb()
import MySQLdb

engine = create_engine("mysql+mysqldb://"+user+":"+str(passwd)+"@"+host+":"+str(port)+"/"+db, encoding='utf-8')
insert_df.to_sql(name='YT_sub_view_trend', con=engine, if_exists='append', index=False)
