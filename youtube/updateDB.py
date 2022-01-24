from requests_html import HTMLSession,AsyncHTMLSession  #!pip install requests_html
from bs4 import BeautifulSoup as bs # importing BeautifulSoup
import nest_asyncio #!pip install nest_asyncio
import pandas as pd
import json
import re
import time
from tqdm import tqdm
import pymysql
from sqlalchemy import create_engine
pymysql.install_as_MySQLdb()
import MySQLdb
import numpy as np
import requests
from datetime import date,datetime
from sqlalchemy import create_engine
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import Insert


session = HTMLSession()


# Connect to the database
connection = pymysql.connect(host=host,
                             user=user,
                             password=passwd,
                             port=port,
                             database=db,
                             charset='utf8')

sql = "SELECT video_id FROM YT_video_lists WHERE genre is NULL;"

cursor = connection.cursor()
cursor.execute(sql)
array_video_id = np.array(cursor.fetchall())


video_cnts = len(array_video_id)

headers = {
            'Referer': 'https://www.youtube.com/',
            'User-Agent':'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36 OPR/58.0.3135.127'
}

update_lists = []
cnt = 0
for video_id in tqdm(array_video_id[500000:800000]): #Video_ID 목록
    cnt += 1
    result = {} #하나의 row에 담을 dictionary
    video_lists = []
    video_url = "https://www.youtube.com/watch?v="+video_id[0]
    response = session.get(video_url,headers = headers) #URL 통신
    if(response.status_code == 429):
        print(response)
    soup = bs(response.text, "html.parser") #beautifulSoup 파싱
    
    #좋아요
    
    try:
        
        data = re.search(r"var ytInitialData = ({.*?});", soup.prettify()).group(1)
        data_json = json.loads(data)
        videoPrimaryInfoRenderer = data_json['contents']['twoColumnWatchNextResults']['results']['results']['contents'][0]['videoPrimaryInfoRenderer']
        videoSecondaryInfoRenderer = data_json['contents']['twoColumnWatchNextResults']['results']['results']['contents'][1]['videoSecondaryInfoRenderer']
        # number of likes
        likes_label = videoPrimaryInfoRenderer['videoActions']['menuRenderer']['topLevelButtons'][0]['toggleButtonRenderer']['defaultText']['accessibility']['accessibilityData']['label'] # "No likes" or "###,### likes"
        like_counts = re.sub(r'[^0-9]', '', likes_label)
        
    except:
        
        like_counts = ''
        
    result['likes'] = like_counts
    
    

    #유료 광고 여부 ads_yn
    
    html = response.text
    
    try:        
        paidContent = int(str(html).index('paidContentOverlayRenderer'))        
    except:        
        result['ads_yn'] = 0
        paidContent = 0
        
    if('유료' in html[paidContent+20:paidContent+100]):
        result['ads_yn'] = 1
    else:
        result['ads_yn'] = 0


    meta = soup.find_all("meta") #메타 기본 정보 불러오기
    
    if (len(meta)>6): # Meta 정보가 7개 이상일 때 크롤링 (비공개 동영상은 정보 없어서 크롤링 불가능)
        result['video_id'] = soup.find("meta", itemprop="videoId")['content']
        result['genre'] = soup.find("meta", itemprop="genre")['content']
        result['duration'] =  soup.find("meta", itemprop="duration")['content']
        video_lists.append(result['likes'])
        video_lists.append(result['duration'])
        video_lists.append(result['genre'])
        video_lists.append(result['ads_yn'])
        video_lists.append(result['video_id'])
        update_lists.append(video_lists)
        
    if(cnt % 9 == 5):
        time.sleep(1)
        if(len(update_lists[4])>1):
            sql = "UPDATE YT_video_lists SET likes = %s,duration = %s,genre = %s,ads_yn = %s WHERE video_id = %s "
            cursor.executemany(sql,update_lists)
            connection.commit()
            update_lists = []
        else:
            time.sleep(1)
            
        
        
