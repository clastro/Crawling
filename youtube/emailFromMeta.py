import pandas as pd
from requests_html import AsyncHTMLSession 
from bs4 import BeautifulSoup as bs # importing BeautifulSoup
import nest_asyncio
import json
import re
import time
nest_asyncio.apply() #jupyter notebook일 경우
import pymysql
from tqdm import tqdm

df_cid['channel_id'] #Youtube 채널 아이디 목록 17000여개

url = "https://www.youtube.com/channel/"

session = AsyncHTMLSession()
mail_pattern = r"([\w\.-]+)@([\w\.-]+)(\.[\w\.]+)" #이메일 패턴 정규식

results = {}

cnt = 0 

for cid in tqdm(df_cid['channel_id']):
    result = {}
    video_url = url+cid
    response = await session.get(video_url)
    soup = bs(response.html.html, "html.parser")
    if(soup.find("meta", itemprop="description") != None):
        description = soup.find("meta", itemprop="description")['content']
    else:
        description = ''
    match = re.search(mail_pattern,description)
    result['channel_id'] = cid
    if(match != None):
        result['email'] = match.group()
    else:
        result['email'] = 'None'
    results[cid] = result
    cnt += 1
    if (cnt % 99 == 0):
        time.sleep(15) #얼마나 걸어줘야 429 에러에서 벗어나는 지 몰라서 테스트 중 (100개 크롤하고 15초 휴식)

pd.DataFrame(results).T.to_csv('./channel_id_emails_1500.csv',encoding='utf-8-sig',index=False) #파일저장
