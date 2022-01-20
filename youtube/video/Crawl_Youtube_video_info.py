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


host=""
port=""
user=""
passwd=""
db=""

session = HTMLSession()

# Connect to the database
connection = pymysql.connect(host=host,
                             user=user,
                             password=passwd,
                             port=port,
                             database=db,
                             charset='utf8')

sql = "SELECT video_id FROM YT_crawl_video_list WHERE insert_time >= %s;"

cursor = connection.cursor()
cursor.execute(sql,date.today().isoformat())
array_video_id = np.array(cursor.fetchall())

video_cnts = len(array_video_id)

headers = {
            'Referer': 'https://www.youtube.com/',
            'User-Agent':'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36 OPR/58.0.3135.127'
}

video_results = {} #비디오 데이터를 넣을 dictionary
cnt = 0
for video_id in tqdm(array_video_id[1111:1234]): #Video_ID 목록
    if(cnt % 9 == 2):
        time.sleep(3)
    cnt += 1
    result = {} #하나의 row에 담을 dictionary
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
        result['channel_id'] = soup.find("meta", itemprop="channelId")['content']
        result['title'] = soup.find("meta", property="og:title")['content']
        result['image'] = soup.find("meta", property="og:image")['content']
        result['genre'] = soup.find("meta", itemprop="genre")['content']
        result['published_date'] = soup.find("meta", itemprop="datePublished")['content']
        view_count = soup.find("meta", itemprop="interactionCount")
        if(view_count != None):
            result['views'] = soup.find("meta", itemprop="interactionCount")['content']
        else:
            result['views'] = 0
        result['duration'] =  soup.find("meta", itemprop="duration")['content']
        for tag in meta:
            if 'name' in tag.attrs.keys() and tag.attrs['name'].strip().lower() in ['description', 'keywords']:
                result[tag.attrs['name']] = tag.attrs['content']
        video_results[video_id[0]] = result
        
final_results = pd.DataFrame(video_results).T
insert_df = final_results[['video_id','channel_id','title','image','published_date','views','likes','duration','genre','ads_yn']]
insert_df.columns = ['video_id','channel_id','title','video_thumbnails_url','publishDate','views','likes','duration','genre','ads_yn']

insert_detail_df = final_results[['video_id','keywords','description']]
insert_detail_df.columns = ['video_id','tags','video_description']


engine = create_engine("mysql+mysqldb://"+user+":"+str(passwd)+"@"+host+":"+str(port)+"/"+db, encoding='utf-8')

try:
    #insert_df.to_sql(name='YT_video_lists', con=engine, if_exists='append', method=insert_on_duplicate)
    insert_df.to_sql(name='YT_temporary_lists', con=engine, if_exists = 'append', index=False)    
    with engine.begin() as cnx:   
        insert_sql = 'INSERT IGNORE INTO YT_video_lists (SELECT * FROM YT_temporary_lists)'
        cnx.execute(insert_sql)
        connection.commit()
    insert_detail_df.to_sql(name='YT_temporary_lists_detail', con=engine, if_exists = 'append', index=False)
    with engine.begin() as cnx: 
        insert_detail_sql = 'INSERT IGNORE INTO YT_video_lists_detail (SELECT * FROM YT_temporary_lists_detail)'
        cnx.execute(insert_detail_sql)
        connection.commit()    
    cursor = connection.cursor()
    del_sql = """
        DELETE FROM YT_crawl_video_list 
            WHERE insert_time <= %s;
        """    
    cursor.execute(del_sql,datetime.now().isoformat())
    connection.commit()
    
    cursor = connection.cursor()
    del_sql2 = """
        DELETE FROM YT_temporary_lists;
        """    
    cursor.execute(del_sql2)
    connection.commit()
    
    cursor = connection.cursor()
    del_sql3 = """
        DELETE FROM YT_temporary_lists_detail;
        """    
    cursor.execute(del_sql3)
    connection.commit()
    
except:
    insert_df.to_csv('./back_YT_video_lists_'+ date.today().isoformat() +'.csv',encoding='utf-8-sig',index=False)
    insert_detail_df.to_csv('./back_YT_video_lists_detail_'+ date.today().isoformat() +'.csv',encoding='utf-8-sig',index=False)
