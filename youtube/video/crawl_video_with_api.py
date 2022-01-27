import pandas as pd
import numpy as np
import time
import re 
import json
import pymysql
import requests
from apiclient.errors import HttpError
from googleapiclient.discovery import build
import urllib.request #Link Access
from pandas.io.json import json_normalize
from tqdm import tqdm

url = 'https://www.youtube.com/watch?v='
api_key = 'xxxxx' #data API v3
youtube = build('youtube', 'v3', developerKey=api_key) #우선 처음 키로 youtube build 실행
df_keys = pd.read_csv('../YoutubeAPIV3Keys.csv',encoding='utf-8') #API Keys 불러오기
key_series = df_keys['Key']

df_list = pd.read_csv('./49000_video_lists_by_crawl.csv',encoding='utf-8-sig') #크롤링을 할 유튜브 비디오 리스트 목록

key_idx = 0 #key 초기값
for video_id in tqdm(df_list['video_id']): #video_list에 video_id가 담겨 있음
    
    video_list = youtube.videos().list(
            part=['snippet','contentDetails','statistics'], #우리가 필요한 데이터는 snippet, contentDetails, statistics안에
            id=video_id #id는 video_id 를 넣어준다
        )
    try:
        response = video_list.execute() # 리스트를 실행 하면 quota 1개가 줄어들면서 response를 받을 수 있다.
    except:
        key_idx += 1 #key의 index를 추가한다.
        continue #에러 발생시 다음 key로 넘어감
        
    df_res = pd.json_normalize(response) #response를 보기 좋게 parsing하는 pandas 함수
    if(df_res['pageInfo.totalResults'][0]>=1): #만약에 results가 있다면
        df = pd.json_normalize(response['items']) #데이터프레임으로 만들어라
    df_results = df_results.append(df) #그런 데이터프레임을 여러개 붙이고
