import requests
from bs4 import BeautifulSoup as bs
import time
import pandas as pd
import nest_asyncio
from requests_html import AsyncHTMLSession 
from tqdm import tqdm
import json
import re
import numpy as np

from multiprocessing import Pool # Pool import하기

df = pd.read_csv('./49000_video_lists_by_crawl.csv',encoding='utf-8-sig')

def get_content(video_id):
    video_url = "https://www.youtube.com/watch?v="+video_id
    #response = await session.get(video_url) #URL 통신
    response = requests.get(video_url)
    #soup = bs(response.html.html, "html.parser")
    soup = bs(response.text, "html.parser")
    meta = soup.find_all("meta") #메타정보 불러오기
    result = {}

    if (len(meta)>7): # Meta 정보가 7개 이상일 때 크롤링 (비공개 동영상은 정보 없어서 크롤링 불가능)
        df_script = pd.json_normalize(json.loads(soup.find_all("script")[40].text[20:-1]))

        if ('contents.twoColumnWatchNextResults.results.results.contents' in df_script):
            df_video_info = pd.json_normalize(df_script['contents.twoColumnWatchNextResults.results.results.contents'][0])
            if('videoPrimaryInfoRenderer.videoActions.menuRenderer.topLevelButtons' in df_video_info):

                like_index = np.mean(df_video_info['videoPrimaryInfoRenderer.videoActions.menuRenderer.topLevelButtons'].dropna().index)            
                df_like = pd.json_normalize(df_video_info['videoPrimaryInfoRenderer.videoActions.menuRenderer.topLevelButtons'][like_index][0])

                if('toggleButtonRenderer.defaultText.accessibility.accessibilityData.label' in df_like):                
                    df_like = df_like['toggleButtonRenderer.defaultText.accessibility.accessibilityData.label']                
                    df_like[0] = df_like[0].replace('없음','0') #좋아요 없음이면 에러나므로 0으로 변경
                    result['likes'] = int("".join(re.findall('\d',df_like[0])))
                else:
                    result['likes'] = 0    

                result['video_id'] = soup.find("meta", itemprop="videoId")['content']
                result['title'] = soup.find("meta", property="og:title")['content']
                result['image'] = soup.find("meta", property="og:image")['content']
                result['published_date'] = soup.find("meta", itemprop="datePublished")['content']
                result['views'] = soup.find("meta", itemprop="interactionCount")['content']
                result['duration'] =  soup.find("meta", itemprop="duration")['content']
                for tag in meta:
                    if 'name' in tag.attrs.keys() and tag.attrs['name'].strip().lower() in ['description', 'keywords']:
                        result[tag.attrs['name']] = tag.attrs['content']
    return result


if __name__=='__main__':
    start_time = time.time()
    pool = Pool(processes=4) # 4개의 프로세스를 사용.
    results = pool.map(get_content, tqdm(df['video_id'][0:5])) # get_content 함수를 넣어줍시다.
    print("--- %s seconds ---" % (time.time() - start_time))
    df_results = pd.DataFrame(results)
    df_results.to_csv('result_sample.csv',encoding='utf-8-sig',index=False)
