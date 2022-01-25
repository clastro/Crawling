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

#nest_asyncio.apply()
host=""
port= 3306
user=""
passwd=""
db=""
conn  = pymysql.connect(host=host,port=port,user=user,passwd=passwd,db=db)  #데이터베이스 연결
cursor = conn.cursor()
conn.set_charset('utf8')
sql = """
    SELECT channel_id FROM youtuber_info
"""
cursor.execute(sql)
df_channel = pd.DataFrame(cursor.fetchall(),columns=[item[0] for item in cursor.description])

n_days = 1 # n일이 넘으면 리스트 업 하지 말 것

id_lists = []
title_lists = []
channel_lists = []
n_counts = 0
for channel_id in tqdm(df_channel['channel_id'][:1000]):
    video_url = "https://www.youtube.com/channel/{}/videos".format(channel_id)
    session = HTMLSession()
    if(n_counts % 9 == 0): #19번 Loop 돌면 
        time.sleep(3)
    response = session.get(video_url)
    n_counts += 1
    if(response.status_code == 429):
        print(response)
    soup = bs(response.html.html, "html.parser")

    try:
        df_movie_script = pd.json_normalize(json.loads(soup.find_all("script")[33].text[20:-1])) #33번째 스크립트
    except:
        continue
    
    try: # 제대로 값이 없을 때는 예외처리
        df_movie_tabs = df_movie_script['contents.twoColumnBrowseResultsRenderer.tabs'][0][1]['tabRenderer']#2번째 탭
        df_movie_lists = pd.json_normalize(df_movie_tabs)
        df_movie_info = pd.json_normalize(pd.json_normalize(df_movie_lists['content.sectionListRenderer.contents'][0])['itemSectionRenderer.contents'][0][0])

        movie_range = len(df_movie_info['gridRenderer.items'][0])

        for i in range(movie_range-1):
            #print(i)
            time_length = df_movie_info['gridRenderer.items'][0][i]['gridVideoRenderer']['publishedTimeText']['simpleText']
            time_length = time_length.replace(" 전","")
            time_length = time_length.replace("스트리밍 ","")
            time_length = time_length.replace("시간: ","")
            time_length = time_length.replace("개월","_30")
            time_length = time_length.replace("일","_1")
            time_length = time_length.replace("시간","_0")
            time_length = time_length.replace("분","_0")
            time_length = time_length.replace("초","_0")
            time_length = time_length.replace("주","_7")
            time_length = time_length.replace("년","_365")
            date_time_length = int(time_length.split("_")[0]) * int(time_length.split("_")[1])

            if(date_time_length>n_days):
                continue
            else:
                video_id = df_movie_info['gridRenderer.items'][0][i]['gridVideoRenderer']['videoId']
                video_title = df_movie_info['gridRenderer.items'][0][i]['gridVideoRenderer']['title']['runs'][0]['text']
                id_lists.append(video_id)
                title_lists.append(video_title)
                channel_lists.append(channel_id)
    except:
        continue

        
df = pd.DataFrame(id_lists,columns=['video_id'])
df['title'] = title_lists
df['channel_id'] = channel_lists

insert_df = df['video_id']

pymysql.install_as_MySQLdb()

engine = create_engine("mysql+mysqldb://"+user+":"+str(passwd)+"@"+host+":"+str(port)+"/"+db, encoding='utf-8')
insert_df.to_sql(name='YT_crawl_video_list', con=engine, if_exists='append', index=False)

print('complete')
