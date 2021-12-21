id_lists = []
title_lists = []
channel_lists = []
for channel_id in tqdm(df_channel['channel_id'][882:]):
    video_url = "https://www.youtube.com/channel/{}/videos".format(channel_id)
    session = AsyncHTMLSession()
    response = await session.get(video_url)
    soup = bs(response.html.html, "html.parser")
    df_movie_script = pd.io.json.json_normalize(json.loads(soup.find_all("script")[33].text[20:-1])) #33번째 스크립트
    
    if('contents.twoColumnBrowseResultsRenderer.tabs' in df_movie_script):    
        if(len(df_movie_script['contents.twoColumnBrowseResultsRenderer.tabs'][0])>1):
            df_movie_tabs = df_movie_script['contents.twoColumnBrowseResultsRenderer.tabs'][0][1]['tabRenderer']#2번째 탭
            df_movie_lists = pd.io.json.json_normalize(df_movie_tabs)
            
            if ('content.sectionListRenderer.contents' in df_movie_lists):
                df_movie_info = pd.io.json.json_normalize(pd.io.json.json_normalize(df_movie_lists['content.sectionListRenderer.contents'][0])['itemSectionRenderer.contents'][0][0])

                if(len(df_movie_info.columns) != 2): ## 영상이 없으면 칼럼 수가 2개가 됨
                    movie_range = len(df_movie_info['gridRenderer.items'][0])

                    for i in range(movie_range-1):
                        #print(i)
                        if('publishedTimeText' in df_movie_info['gridRenderer.items'][0][i]['gridVideoRenderer']):
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
                            if(date_time_length>30):#30일이 넘으면 리스트 업 하지 말 것
                                continue
                            else:
                                video_id = df_movie_info['gridRenderer.items'][0][i]['gridVideoRenderer']['videoId']
                                video_title = df_movie_info['gridRenderer.items'][0][i]['gridVideoRenderer']['title']['runs'][0]['text']
                                id_lists.append(video_id)
                                title_lists.append(video_title)
                                channel_lists.append(channel_id)

df = pd.DataFrame(id_lists,columns=['video_id'])
df['title'] = title_lists
df['channel_id'] = channel_lists      

df.to_csv('video_list_to_crawl.csv',encoding='utf-8-sig',index=False)
    
