from requests_html import HTMLSession,AsyncHTMLSession  #!pip install requests_html
from bs4 import BeautifulSoup as bs # importing BeautifulSoup
import nest_asyncio #!pip install nest_asyncio
import pandas as pd
import json
import re
import time
from tqdm import tqdm
import pymysql
from datetime import date
from sqlalchemy import create_engine

host=""
port=3306
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

id_lists = []
view_lists = []
day_lists = []
channel_lists = []
n_counts = 0
for channel_id in tqdm(df_channel['channel_id']):
    video_url = "https://www.youtube.com/channel/{}/videos".format(channel_id)
    session = HTMLSession()
    if(n_counts % 9 == 0): #19번 Loop 돌면 
        time.sleep(3)
    response = session.get(video_url)
    n_counts += 1
    if(response == 429):
        print(response)
    soup = bs(response.html.html, "html.parser")

    try:
        df_movie_script = pd.json_normalize(json.loads(soup.find_all("script")[33].text[20:-1])) #33번째 스크립트
    except:
        continue
    try:
        df_movie_tabs = df_movie_script['contents.twoColumnBrowseResultsRenderer.tabs'][0][1]['tabRenderer']#2번째 탭
        df_movie_lists = pd.json_normalize(df_movie_tabs)
        
        df_movie_info = pd.json_normalize(pd.json_normalize(df_movie_lists['content.sectionListRenderer.contents'][0])['itemSectionRenderer.contents'][0][0])

        movie_range = len(df_movie_info['gridRenderer.items'][0])

        for i in range(movie_range-1):
            #print(i)            
            video_id = df_movie_info['gridRenderer.items'][0][i]['gridVideoRenderer']['videoId']
            try:
                video_views = df_movie_info['gridRenderer.items'][0][i]['gridVideoRenderer']['viewCountText']['simpleText']
            except:
                video_views = ''
            video_crawl_days = date.today().isoformat()
            id_lists.append(video_id)
            view_lists.append(video_views)
            channel_lists.append(channel_id)
            day_lists.append(video_crawl_days)      
    except:
        continue

                            
insert_df = pd.DataFrame(id_lists,columns=['video_id'])
insert_df['channel_id'] = channel_lists
insert_df['views'] = view_lists
insert_df['date'] = day_lists
insert_df['views'] = insert_df['views'].apply(lambda x : re.sub(r'[^0-9]', '',x))
pymysql.install_as_MySQLdb()
import MySQLdb

engine = create_engine("mysql+mysqldb://"+user+":"+str(passwd)+"@"+host+":"+str(port)+"/"+db, encoding='utf-8')
insert_df.to_sql(name='YT_video_view_trend', con=engine, if_exists='append', index=False)
    
    
    
