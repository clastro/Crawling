
channel_list = []

cnt = 0

for channel_id in tqdm(df_channel['channel_id']): #Video_ID 목록
    cnt += 1
    result = {} #하나의 row에 담을 dictionary
    result_list = []
    url = "https://www.youtube.com/channel/"+channel_id
    response = await session.get(url,headers = headers) #URL 통신
    if(response.status_code == 429):
        print(response)
    soup = bs(response.text, "html.parser") #beautifulSoup 파싱
    
    meta = soup.find_all("meta") #메타정보 불러오기
    try:
        result['thumb'] = soup.find("meta", property="og:image")['content']
    except:
        result['thumb'] = ''

    html = response.text

    try:        
        bannerContent = int(str(html).index('"banner":{"thumbnails":[{"url":'))        
        result['banner'] = html[bannerContent+10:bannerContent+350].split(',')[0].split('"thumbnails":[{"url":"')[1]
    except:        
        result['banner'] = ''
        bannerContent = 0
    
    
    #print(len(meta))
    
    if (len(meta)>6): # Meta 정보가 7개 이상일 때 크롤링 (비공개 동영상은 정보 없어서 크롤링 불가능)
       
         
        result_list.append(result['thumb'])
        result_list.append(result['banner'])
        result_list.append(channel_id)
        channel_list.append(result_list)
        
    #print(video_id)
    
    if(cnt % 9 == 7):
        time.sleep(1.9)
        if (len(channel_list)>0):
            sql = "UPDATE youtuber_info SET thumbnail =%s, banner = %s WHERE channel_id = %s"
            #print(sql)
            cursor.executemany(sql, channel_list)
            conn.commit()
            video_list = []
        else:
            time.sleep(2.2)
