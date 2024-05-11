import mysql
import mysql.connector
import pandas as pd
from pprint import pprint
import googleapiclient.discovery
import streamlit as st
import pymongo
from googleapiclient.errors import HttpError



api_service_name = "youtube"
api_version = "v3"
api_key="AIzaSyD6NykGhsbfXaLoVbTgR9qHJwVSroAW1mQ"

youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey=api_key)

def channel_detail(channel_id):
  request = youtube.channels().list(
       part="snippet,contentDetails,statistics,contentOwnerDetails,status",
       id=channel_id    )
  channel_details = request.execute()

  x=dict(channel_id=channel_details['items'][0]['id'],
        playlist_id=channel_details['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
        channel_title=  channel_details['items'][0]['snippet']['title'],
        channel_videoCount=channel_details['items'][0]['statistics']['videoCount'],
        channel_viewCount=channel_details['items'][0]['statistics']['viewCount'],
        channel_subscriberCount=channel_details['items'][0]['statistics']['subscriberCount'],
        channel_opened_date=channel_details['items'][0]['snippet']['publishedAt'],
        channel_thumbnails= channel_details['items'][0]['snippet']['thumbnails']['medium']['url'],
        channel_desc= channel_details['items'][0]['snippet']['description'],
        channel_status=channel_details['items'][0]['status']['privacyStatus'] )
  return x

#print(channel_detail)
#insert=channel_detail('UCWpi_d57idpdDesRM7_9Agg')

#video ids

def all_video_ids(channel_id):
       video_ids=[]
       request4=youtube.channels().list(
              part="contentDetails",
              id=channel_id).execute()

       Playlist_Id=request4['items'][0]['contentDetails']['relatedPlaylists']['uploads']
       print(Playlist_Id)

       next_page_token=None
       while True:
              
              request5=youtube.playlistItems().list(
                     part="snippet",
                     maxResults=50,
                     pageToken=next_page_token,
                     playlistId=Playlist_Id).execute()
              next_page_token=request5.get('nextPageToken')

              
              for i in range(len(request5['items'])):
                     video_id=request5['items'][i]['snippet']['resourceId']['videoId']
                     video_ids.append(video_id)
              if next_page_token is None:
                     break
       return video_ids


def video_detail(video_ids):
  all_video_details=[]
  for j in video_ids:
    request_2 = youtube.videos().list(
        part="snippet,contentDetails,statistics",
        id=j   )
    video_details = request_2.execute()

    for i in video_details['items']:
        y=dict(video_id=i['id'],
               channel_name=i['snippet']['channelTitle'],
              channel_id=i['snippet']['channelId'],
              video_duration=i['contentDetails']['duration'],
              video_published=i['snippet']['publishedAt'],
              video_name=i['snippet']['title'],
              video_comment_count=i['statistics'].get('commentCount'),
              video_like_count=i['statistics'].get('likeCount'),
              video_fav_count=i['statistics'].get('favoriteCount'),
              video_view_count=i['statistics']['viewCount'],
              video_desc=i['snippet']['localized']['description'],            
              video_caption_status=i['contentDetails']['caption'])
        all_video_details.append(y)
  return all_video_details

#Video_info=video_detail(video_ids)
#pprint(Video_info)


def comment_detail(video_ids):
    all_comments = []
    for k in video_ids:
        try:
            request_3 = youtube.commentThreads().list(
                part="snippet,replies",
                videoId=k)
            comment_details = request_3.execute()
            
            for each in comment_details.get('items', []):
                z = dict(
                    channel_id= each['snippet']['topLevelComment']['snippet']['channelId'],
                    comment_id= each['id'], 
                    video_id= each['snippet']['topLevelComment']['snippet']['videoId'],
                    comment_date= each['snippet']['topLevelComment']['snippet']['publishedAt'],
                    comment_like= each['snippet']['topLevelComment']['snippet']['likeCount'],
                    comment_author= each['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    comment_text= each['snippet']['topLevelComment']['snippet']['textOriginal']
                )
                all_comments.append(z)
        except HttpError as e:
            if e.resp.status == 403:
                # Comments are disabled for this video, so skip it
                print(f"Comments are disabled for video with ID {k}. Skipping...")
            else:
                # Handle other HTTP errors
                print(f"Error fetching comments for video with ID {k}: {e}")
    
    return all_comments


#comments_info=comment_detail(Video_ids)
#print(comments_info)



#client = pymongo.MongoClient("mongodb://mongodb_server_ip:27017/")
from pymongo import MongoClient
import dns.resolver

dns.resolver.default_resolver=dns.resolver.Resolver(configure=False)
dns.resolver.default_resolver.nameservers=['8.8.8.8']

client=pymongo.MongoClient ("mongodb+srv://andrea10ivana:andyarjun@cluster0.zn6ev1w.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db=client['youtube']





def full_data(channel_id):
    ch=channel_detail(channel_id)
    all_vi=all_video_ids(channel_id)
    vi=video_detail(all_vi)
    com=comment_detail(all_vi)

    collection=db['Final_data']
    collection.insert_one({'channel_info':ch,'video_info':vi,'comments_info':com})    

    return "Records uploaded"


#full_data('UCzNq9i-DlDDBLjPerVzJW-A')
#full_data('UCTAvhe5tqlkOJh8u-HufnIw')
#full_data('UCWpi_d57idpdDesRM7_9Agg')

#UCWpi_d57idpdDesRM7_9Agg LIBA 188 videos
#UCTAvhe5tqlkOJh8u-HufnIw premji 42 vid
#UCzNq9i-DlDDBLjPerVzJW-A kenny 335 videos


def Channel_MONGO_2_SQL(channel_s):
    mysql_connection = mysql.connector.connect(
        host="localhost",
        user="root",
        port='3006',
        password="Andy_123",
        database="youtube"
    )
    mysql_cursor = mysql_connection.cursor()


    query='''create table if not exists channel_details_table (channel_id VARCHAR(50) primary key,
                playlist_id VARCHAR(100),
                channel_title VARCHAR(100),
                channel_videoCount INT,
                channel_viewCount INT,
                channel_subscriberCount INT,
                channel_opened_date VARCHAR(100),
                channel_thumbnails VARCHAR(200),
                channel_desc TEXT,
                channel_status VARCHAR(100))'''
    mysql_cursor.execute(query)
    mysql_connection.commit()

    print('channel_details_table table already created')


    single_channel_detail=[]
    db=client['youtube']
    collection=db['Final_data']
    for each in collection.find({'channel_info.channel_title':channel_s},{'_id':0}):
        single_channel_detail.append(each['channel_info'])  


    df_single_channel_detail=pd.DataFrame(single_channel_detail)

    for index, row in df_single_channel_detail.iterrows():        
        query = '''INSERT INTO channel_details_table 
                    (channel_id, playlist_id, channel_title, channel_videoCount, channel_viewCount, 
                    channel_subscriberCount, channel_opened_date, channel_thumbnails, channel_desc, channel_status) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
        values = (row['channel_id'], row['playlist_id'], row['channel_title'], row['channel_videoCount'],
                row['channel_viewCount'], row['channel_subscriberCount'], row['channel_opened_date'],
                row['channel_thumbnails'], row['channel_desc'], row['channel_status'])
        
        try:
            mysql_cursor.execute(query, values)
            mysql_connection.commit()

        except:
            msg= f'Channel name {channel_s} already exists'
            return msg
       


def Video_MONGO_2_SQL(channel_s):
    mysql_connection = mysql.connector.connect(
        host="localhost",
        user="root",
        port='3006',
        password="Andy_123",
        database="youtube"
    )
    mysql_cursor = mysql_connection.cursor()
        


    query='''create table if not exists video_details_table (video_id VARCHAR(50) primary key ,
                channel_name VARCHAR(100),
                channel_id VARCHAR(100),
                video_duration VARCHAR(100),
                video_published VARCHAR(50),
                video_name TEXT,
                video_comment_count INT,
                video_like_count INT,
                video_fav_count INT,
                video_view_count INT,
                video_desc TEXT,
                video_caption_status VARCHAR(100))'''
    mysql_cursor.execute(query)
    mysql_connection.commit()


    


    single_list_video=[]
    db=client['youtube']
    collection=db['Final_data']
    for each in collection.find({'channel_info.channel_title':channel_s},{'_id':0}):    
        for i in range(len(each['video_info'])):    
            single_list_video.append(each['video_info'][i]) 
        
    single_video_df=pd.DataFrame(single_list_video)
    
   

    for index, row in single_video_df.iterrows():
        
        query1 = '''INSERT INTO video_details_table 
                    (video_id , channel_name, channel_id ,  video_duration , video_published , video_name ,video_comment_count ,
                    video_like_count ,video_fav_count , video_view_count , video_desc , video_caption_status ) 
                    VALUES (%s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
        values = (row['video_id'], row['channel_name'], row['channel_id'], row['video_duration'], row['video_published'],
                row['video_name'], row['video_comment_count'], row['video_like_count'],
                row['video_fav_count'], row['video_view_count'], row['video_desc'],row['video_caption_status'])
        mysql_cursor.execute(query1, values)
        mysql_connection.commit()






def Comment_MONGO_2_SQL(channel_s):
    mysql_connection = mysql.connector.connect(
        host="localhost",
        user="root",
        port='3006',
        password="Andy_123",
        database="youtube"
    )
    mysql_cursor = mysql_connection.cursor()

        

    query='''create table if not exists comment_details_table (channel_id VARCHAR(50),
                        comment_id VARCHAR(50) primary key,
                        video_id VARCHAR(50),
                        comment_date VARCHAR(50), 
                        comment_like INT,
                        comment_author VARCHAR(50),
                        comment_text TEXT)'''
    mysql_cursor.execute(query)
    mysql_connection.commit()
    


    single_list_comment=[]
    db=client['youtube']
    collection=db['Final_data']
    for each in collection.find({'channel_info.channel_title':channel_s},{'_id':0}):
        for i in range(len(each['comments_info'])):
            single_list_comment.append(each['comments_info'][i]) 
    single_comment_df=pd.DataFrame(single_list_comment)

    for index, row in single_comment_df.iterrows():
        
        query2 = '''INSERT INTO comment_details_table 
                    (channel_id,comment_id,video_id,comment_date, comment_like,comment_author,comment_text ) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s)'''
        values = (row['channel_id'], row['comment_id'], row['video_id'], row['comment_date'],
                row['comment_like'], row['comment_author'], row['comment_text'])
        mysql_cursor.execute(query2, values)
        mysql_connection.commit()


def tables(channel):
    msg=Channel_MONGO_2_SQL(channel)
    if msg:
        return msg
    else:
        Video_MONGO_2_SQL(channel)
        Comment_MONGO_2_SQL(channel)    
    
        return "Tables created"

#tables=tables()


def St_show_channels_table():    
    list_channel=[]
    db=client['youtube']
    collection=db['Final_data']
    for each in collection.find({},{'_id':0,'channel_info':1}):
        list_channel.append(each['channel_info'])  
    df=st.dataframe(list_channel)
    
    return df


def St_show_videos_table():
    list_video=[]
    db=client['youtube']
    collection=db['Final_data']
    for each in collection.find({},{'_id':0,'video_info':1}):
        for i in range(len(each['video_info'])):
            list_video.append(each['video_info'][i]) 
    df1=st.dataframe(list_video)

    return df1


def St_show_comments_table():
    list_comment=[]
    db=client['youtube']
    collection=db['Final_data']
    for each in collection.find({},{'_id':0,'comments_info':1}):
        for i in range(len(each['comments_info'])):
            list_comment.append(each['comments_info'][i]) 
    df2=st.dataframe(list_comment)

    return df2


channel_id = st.text_input("Enter channel id")
if st.button('Add channel'):
    input_list_channel=[]
    db=client['youtube']
    collection=db['Final_data']
    for each in collection.find({},{'_id':0,'channel_info':1}):
        input_list_channel.append(each['channel_info']['channel_id'])  
    
    if channel_id in input_list_channel:
        st.success("Channel already iputed")
    else:
        insert1=full_data(channel_id)
        st.success(insert1)

all_channels=[]
db=client['youtube']
collection=db['Final_data']
for each in collection.find({},{'_id':0,'channel_info':1}):
    all_channels.append(each['channel_info']['channel_title'])


unique_channel=st.selectbox('Select the channel',all_channels)

if st.button('To SQL'):
    table=tables(unique_channel)
    st.success(table)


show_table=st.radio('SELECT INFO YOU WANT',("CHANNEL","VIDEO","COMMENTS"))

if show_table=='CHANNEL':
    St_show_channels_table()

elif show_table=='VIDEO':
    St_show_videos_table()

elif show_table=='COMMENTS':
    St_show_comments_table()






##SQL VIEW IN STREAMLIT

mysql_connection = mysql.connector.connect(
        host="localhost",
        user="root",
        port='3006',
        password="Andy_123",
        database="youtube"
    )
mysql_cursor = mysql_connection.cursor()

question=st.selectbox("Questions",["1.names of all the videos and their corresponding channels?",
                                "2.channels have the most number of videos, and how many videos do they have?",
                                "3. top 10 most viewed videos and their respective channels",
                                "4. How many comments were made on each video, and what are their corresponding video names?",
                                "5.Which videos have the highest number of likes, and what are corresponding channel names?",
                                "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                "7.What is the total number of views for each channel, and what are their corresponding channel names?",
                                "8.What are the names of all the channels that have published videos in the year 2022?",
                                "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                "10.Which videos have the highest number of comments, and what are their corresponding channel names?"])


if question=="1.names of all the videos and their corresponding channels?":
    query1='''select video_name , channel_name 
                from video_details_table '''
    mysql_cursor.execute(query1)
    #mysql_connection.commit()
    t1=mysql_cursor.fetchall()
    df1=pd.DataFrame(t1,columns=['Video_name','channel_name'])
    st.write(df1)


elif question=="2.channels have the most number of videos, and how many videos do they have?":
    query2='''select channel_title , channel_videoCount 
                from channel_details_table 
                order by channel_videoCount desc  '''
    mysql_cursor.execute(query2)
    #mysql_connection.commit()
    t2=mysql_cursor.fetchall()
    df2=pd.DataFrame(t2,columns=['channel_title','channel_videoCount'])
    st.write(df2)
    mysql_cursor.close()

elif question=="3. top 10 most viewed videos and their respective channels":
    query3='''select video_name , video_view_count ,channel_name 
                from video_details_table 
                where video_view_count is not null
                order by video_view_count desc                  
                limit 10 '''
    mysql_cursor.execute(query3)
    #mysql_connection.commit()
    t3=mysql_cursor.fetchall()
    df3=pd.DataFrame(t3,columns=['video_name','video_view_count','channel_name'])
    st.write(df3)
    mysql_cursor.close()

elif question=="4. How many comments were made on each video, and what are their corresponding video names?":
    query4='''select video_name , video_comment_count 
                from video_details_table 
                 '''
    mysql_cursor.execute(query4)
    #mysql_connection.commit()
    t4=mysql_cursor.fetchall()
    df4=pd.DataFrame(t4,columns=['video_name','video_comment_count'])
    st.write(df4)
    mysql_cursor.close()

elif question=="5.Which videos have the highest number of likes, and what are corresponding channel names?":
    query5='''select video_name , video_like_count ,channel_name
                from video_details_table 
                order by video_like_count desc
                 '''
    mysql_cursor.execute(query5)
    #mysql_connection.commit()
    t5=mysql_cursor.fetchall()
    df5=pd.DataFrame(t5,columns=['video_name','video_like_count','channel_name'])
    st.write(df5)
    mysql_cursor.close()

    
elif question=="6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
    query6='''select video_name , video_like_count 
                from video_details_table 
                                 '''
    mysql_cursor.execute(query6)
    #mysql_connection.commit()
    t6=mysql_cursor.fetchall()
    df6=pd.DataFrame(t6,columns=['video_name','video_like_count'])
    st.write(df6)
    mysql_cursor.close()
#need dislike count in video table

elif question=="7.What is the total number of views for each channel, and what are their corresponding channel names?":
    query7='''select channel_title,channel_viewCount
                from channel_details_table 
                                 '''
    mysql_cursor.execute(query7)
    #mysql_connection.commit()
    t7=mysql_cursor.fetchall()
    df7=pd.DataFrame(t7,columns=['channel_title','channel_viewCount'])
    st.write(df7)
    mysql_cursor.close()

elif question=="8.What are the names of all the channels that have published videos in the year 2022?":
    query8='''select channel_title,substr(channel_opened_date,1,4) as published_yr
                from channel_details_table 
                where substr(channel_opened_date,1,4)='2022'
                                 '''
    mysql_cursor.execute(query8)
    #mysql_connection.commit()
    t8=mysql_cursor.fetchall()
    df8=pd.DataFrame(t8,columns=['channel_title','published_yr'])
    st.write(df8)
    mysql_cursor.close()

elif question=="9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    query9='''select channel_name,AVG(
        TIME_TO_SEC( CONCAT( SUBSTRING_INDEX(SUBSTRING_INDEX(video_duration, 'H', 1), 'T', -1), ':', 
                SUBSTRING_INDEX(SUBSTRING_INDEX(video_duration, 'M', 1), 'H', -1), ':', 
                SUBSTRING_INDEX(video_duration, 'S', 1) ) ) / 60) AS avg_dur_minutes
            from video_details_table
            group by channel_name'''
    mysql_cursor.execute(query9)
    #mysql_connection.commit()
    t9=mysql_cursor.fetchall()
    df9=pd.DataFrame(t9,columns=['channel_name','avg_dur'])
    st.write(df9)
    mysql_cursor.close()

elif question=="10.Which videos have the highest number of comments, and what are their corresponding channel names?":
    query10='''select channel_name, video_name,video_comment_count
                from video_details_table     
                order by  video_comment_count desc          
                                 '''
    mysql_cursor.execute(query10)
    #mysql_connection.commit()
    t10=mysql_cursor.fetchall()
    df10=pd.DataFrame(t10,columns=['channel_name','video_name','video_comment_count'])
    st.write(df10)
    mysql_cursor.close()


#UCbCmjCuTUZos6Inko4u57UQ
#UC3rLoj87ctEHCcS7BuvIzkQ
#UCbertc-gMbkkHuSmg0qwnxw
#UCWpi_d57idpdDesRM7_9Agg LIBA 188 videos
#UCTAvhe5tqlkOJh8u-HufnIw premji 42 vid
#UCzNq9i-DlDDBLjPerVzJW-A kenny 335 videos
