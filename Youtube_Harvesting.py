from googleapiclient.discovery import build
import pandas as pd
import streamlit as st
import psycopg2
import pymongo
from pprint import pprint

# API Key connection 
def api_conn():
    api_id = "AIzaSyBxwGOkVWhOl0rjeukp_9xJw7pIeHds4h4"
    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name,api_version,developerKey=api_id)
    return youtube

yt= api_conn()

# channel details 
def channal_info(channel_id):
    request = yt.channels().list(
                part = "snippet,statistics,contentDetails,topicDetails,contentOwnerDetails",
                id = channel_id
                )
    responce = request.execute()

    for i in responce['items']:
        ch = dict(
            ch_name = i ['snippet']['title'],
            ch_id = i ['id'],
            ch_description = i['snippet']['description'],
            ch_published = i['snippet']['publishedAt'],
            ch_subscriberCount = i ['statistics']['subscriberCount'],
            ch_videocount = i ['statistics']['videoCount'],
            ch_viewcount = i ['statistics']['viewCount'],
            ch_playlist = i ['contentDetails']['relatedPlaylists']['uploads']
        )
    return ch 

# playlist details 
def playlist_info(channel_id):
    
    while True:
        pl_data = []
        next_page_token = None
        r1 = yt.playlists().list(
            part = "snippet,contentDetails",
            channelId = channel_id,
            maxResults = 50,
            pageToken = next_page_token
        ).execute()
        for i in r1['items']:
            pl = dict(
                    ch_name = i ['snippet']['channelTitle'],
                    ch_id = i ['snippet']['channelId'],
                    pl_id = i ['id'],
                    pl_name = i ['snippet']['title'],
                    pl_description = i ['snippet'].get('description'),
                    pl_publishedat = i ['snippet']['publishedAt'],
                    vi_count = i ['contentDetails']['itemCount']
            )
            pl_data.append(pl)
        next_page_token = r1.get("nextPageToken" )

        if next_page_token == None:
            break
    return pl_data

# video ids
def get_videos_ids(channel_id):
    video_ids = []
    r2 = yt.channels().list(
        id = channel_id,
        part = "contentDetails"
    ).execute()

    playli_id = r2['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None 

    while True: 
        r3 = yt.playlistItems().list(
            part = "snippet",
            playlistId = playli_id,
            maxResults = 50,
            pageToken = next_page_token
        ).execute()
        for i in range(len(r3['items'])):
            video_ids.append(r3['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = r3.get('nextPageToken')
        if next_page_token is None :
            break 
    return video_ids

# getting video details 
def get_video_info(video_ids):
    video_data = []
    for video_id in video_ids:
        r4 = yt.videos().list(
            part = "snippet,contentDetails,statistics,topicDetails",
            id = video_id
        ).execute()

        for i in r4['items']:
            vi_data = dict(
                    ch_id = i ['snippet']['channelId'],
                    ch_name = i ['snippet']['channelTitle'],
                    video_id = i ['id'],
                    vi_title = i ['snippet']['title'],
                    vi_description = i ['snippet']['description'],
                    vi_published = i ['snippet']['publishedAt'],
                    vi_duration = i ['contentDetails']['duration'],
                    vi_viewcount = i ['statistics'].get('viewCount'),
                    vi_likecount = i ['statistics'].get('likeCount'),
                    vi_cmt_count = i ['statistics'].get('commentCount')
            )
            video_data.append(vi_data)
    return video_data

# get video comments
def get_comt_info (video_ids):
    comment_data = []
    try :
        for vi_cmt in video_ids:
            r5 =yt.commentThreads().list(
                part =" snippet",
                videoId = vi_cmt,
                maxResults = 100
            ).execute()

            for i in r5['items']:
                cmt_data = dict(
                            ch_id = i ['snippet']['channelId'],
                            vid_id = i ['snippet']['videoId'],
                            cmt_id = i ['snippet']['topLevelComment']['id'],
                            cmt_author_name = i ['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            cmt_author_id = i ['snippet']['topLevelComment']['snippet']['authorChannelId']['value'],
                            cmt_displayed = i ['snippet']['topLevelComment']['snippet']['textDisplay'],
                            cmt_published = i ['snippet']['topLevelComment']['snippet']['publishedAt'],
                            cmt_likes = i ['snippet']['topLevelComment']['snippet'].get('likeCount'),
                )
                comment_data.append(cmt_data)
    except: 
        pass 
    return comment_data

#insert into mongodb
mongodb_url = "mongodb://localhost:27017"
db_name = "youtube_project"
client =pymongo.MongoClient(mongodb_url)
db = client[db_name]
coll_name = "YT_harvesting"
collection =db[coll_name]

def channel_details (channel_id):
    ch_details = channal_info(channel_id)
    pl_details = playlist_info(channel_id)
    vi_id_details = get_videos_ids(channel_id)
    vi_details = get_video_info(vi_id_details)
    cm_details = get_comt_info(vi_id_details)


    coll1 = db["channel_details"]
    coll1.insert_one({"channel_info":ch_details,
                    "Playlist_info": pl_details,
                    "VideoDetails_info": vi_details,
                    "comment_info":cm_details})
    return

#Channel table

def channels_table():

    mydb = psycopg2.connect( host = "localhost",
                            user = "postgres",
                            password = "Mj@2590",
                            database = "youtube_data",
                            port = 5432)
    cursor = mydb.cursor()

    drop_query = '''drop table if exists channels'''
    cursor.execute(drop_query)
    mydb.commit()


    create_query =  '''create table if not exists channels( ch_name varchar(1000),
                                                            ch_id varchar(1000) primary key,
                                                            ch_description text,
                                                            ch_subscriberCount int,
                                                            ch_videocount int,
                                                            ch_viewcount bigint,
                                                            ch_playlist varchar(1000))'''
    cursor.execute(create_query)
    mydb.commit()

    ch_list=[]
    db = client["youtube_project"]
    coll1 = db["channel_details"]

    for ch_data in coll1.find({},{"_id":0,"channel_info":1}):
        ch_list.append(ch_data["channel_info"])

    ch_df = pd.DataFrame(ch_list)

    for index, row in ch_df.iterrows():
        insert_query = '''insert into channels(
                                                ch_id,
                                                ch_name,
                                                ch_description,
                                                ch_subscriberCount,
                                                ch_videocount,
                                                ch_viewcount,
                                                ch_playlist)
                                                
                                                values(%s,%s,%s,%s,%s,%s,%s)'''
        values = (row['ch_name'],
                row['ch_id'],
                row['ch_description'],
                row['ch_subscriberCount'],
                row['ch_videocount'],
                row['ch_viewcount'],
                row['ch_playlist'])

        try: 
            cursor.execute(insert_query,values)
            mydb.commit()
        except:
            print("channel")

#plylist tables--->

def playlist_table():
    mydb = psycopg2.connect( host = "localhost",
                            user = "postgres",
                            password = "Mj@2590",
                            database = "youtube_data",
                            port = "5432")
    cursor = mydb.cursor()

    drop_query1= '''drop table if exists playlist'''
    cursor.execute(drop_query1)
    mydb.commit()

    create_query1 = '''create table if not exists playlist (ch_name varchar(1000),
                                                            ch_id varchar(1000),
                                                            pl_id varchar(1000) primary key,
                                                            pl_name varchar(1000),
                                                            pl_description text,
                                                            pl_publishedat timestamp,
                                                            vi_count int)'''
    cursor.execute(create_query1)
    mydb.commit()

    pls_list=[]
    db = client["youtube_project"]
    coll1 = db["channel_details"]

    for pls_data in coll1.find({},{"_id":0,"Playlist_info":1}):
        for i in range(len(pls_data["Playlist_info"])):
            pls_list.append(pls_data["Playlist_info"][i])
    pls_df = pd.DataFrame(pls_list)

    for index, row in pls_df.iterrows():
        insert_query1 = '''insert into playlist(
                                                ch_name,
                                                ch_id,
                                                pl_id,
                                                pl_name,
                                                pl_description,
                                                pl_publishedat,
                                                vi_count)
                                                values(%s,%s,%s,%s,%s,%s,%s)'''
        values1= (row['ch_name'],
                row['ch_id'],
                row['pl_id'],
                row['pl_name'],
                row['pl_description'],
                row['pl_publishedat'],
                row['vi_count'])

        mydb.rollback()
        cursor.execute(insert_query1,values1)
        mydb.commit()

# Video tables--->

def video_table():

    mydb = psycopg2.connect( host = "localhost",
                        user = "postgres",
                        password = "Mj@2590",
                        database = "youtube_data",
                        port = "5432")
    cursor = mydb.cursor()

    drop_query3 = '''drop table if exists videos'''
    cursor.execute(drop_query3)
    mydb.commit()


    create_query3 ='''create table if not exists videos (ch_id varchar(1000),
                                                        ch_name varchar(1000),
                                                        video_id varchar(100) primary key,
                                                        vi_title varchar(1000),
                                                        vi_duration interval,
                                                        vi_description text,
                                                        vi_published timestamp,
                                                        vi_viewcount bigint,
                                                        vi_likecount bigint,
                                                        vi_cmt_count int
                                                        )'''

    cursor.execute(create_query3)
    mydb.commit()

    vis_list=[]
    db = client["youtube_project"]
    coll1 = db["channel_details"]

    for vi_data in coll1.find({},{"_id":0,"VideoDetails_info":1}):
        for i in range(len(vi_data["VideoDetails_info"])):
            vis_list.append(vi_data["VideoDetails_info"][i])
    vi_df = pd.DataFrame(vis_list)
    for index, row in vi_df.iterrows():
        insert_query2 = '''insert into videos(ch_id, 
                                            ch_name,
                                            video_id,
                                            vi_title,
                                            vi_duration,
                                            vi_description,
                                            vi_published,
                                            vi_viewcount,
                                            vi_likecount,
                                            vi_cmt_count)
                                            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        values2 = (row['ch_id'],
                row['ch_name'],
                row['video_id'],
                row['vi_title'],
                row['vi_duration'],
                row['vi_description'],
                row['vi_published'],
                row['vi_viewcount'],
                row['vi_likecount'],
                row['vi_cmt_count'])

        
        mydb.rollback()
        cursor.execute(insert_query2,values2)
        
        mydb.commit()

# Comments tables ---->

def comment_table():
    mydb = psycopg2.connect(host="localhost",
                user="postgres",
                password="Mj@2590",
                database= "youtube_data",
                port = "5432"
                )
    cursor = mydb.cursor()

    drop_query4 = "drop table if exists comments"
    cursor.execute(drop_query4)
    mydb.commit()

    create_query4 = '''CREATE TABLE if not exists comments(cmt_id varchar(1000) primary key,
                                        ch_id varchar(100),
                                        vid_id varchar(1000),
                                        cmt_displayed text,
                                        cmt_author_name varchar(1000),
                                        cmt_author_id varchar(100),
                                        cmt_published timestamp,
                                        cmt_likes bigint)
                                        '''
    cursor.execute(create_query4)
    mydb.commit()

    cmt_list=[]
    db = client["youtube_project"]
    coll1 = db["channel_details"]

    for comt_data in coll1.find({},{"_id":0,"comment_info":1}):
        for i in range(len(comt_data["comment_info"])):
            cmt_list.append(comt_data["comment_info"][i])
    df = pd.DataFrame(cmt_list)

    for index, row in df.iterrows():
        insert_query4 = ''' insert into comments(cmt_id,
                                                ch_id,
                                                vid_id,
                                                cmt_displayed,
                                                cmt_author_name,
                                                cmt_author_id,
                                                cmt_published,
                                                cmt_likes) 
                                                values(%s,%s,%s,%s,%s,%s,%s,%s)'''
        values4= (row['cmt_id'],
                row['ch_id'],
                row['vid_id'],
                row['cmt_displayed'],
                row['cmt_author_name'],
                row['cmt_author_id'],
                row['cmt_published'],
                row['cmt_likes'])
        
        mydb.rollback()
        cursor.execute(insert_query4,values4)
        mydb.commit()

#table collections--> 

def tables():
    channels_table()
    playlist_table()
    video_table()
    comment_table()
    return "Tables created Successfully"

# St--->channel df
def show_channel_table():
    ch_list=[]
    db = client["youtube_project"]
    coll1 = db["channel_details"]

    for ch_data in coll1.find({},{"_id":0,"channel_info":1}):
        ch_list.append(ch_data["channel_info"])

    ch_df_table = st.dataframe(ch_list)
    return ch_df_table

# st---> playlist df
def show_playlist_table():
    pls_list=[]
    db = client["youtube_project"]
    coll1 = db["channel_details"]

    for pls_data in coll1.find({},{"_id":0,"Playlist_info":1}):
        for i in range(len(pls_data["Playlist_info"])):
            pls_list.append(pls_data["Playlist_info"][i])
    pl_df_table = st.dataframe(pls_list)
    return pl_df_table

# st---> video df
def show_video_table():
    vis_list=[]
    db = client["youtube_project"]
    coll1 = db["channel_details"]

    for vi_data in coll1.find({},{"_id":0,"VideoDetails_info":1}):
        for i in range(len(vi_data["VideoDetails_info"])):
            vis_list.append(vi_data["VideoDetails_info"][i])
    vi_df_table = st.dataframe(vis_list)
    return vi_df_table

# st---> comment df
def show_comment_table():
    cmt_list=[]
    db = client["youtube_project"]
    coll1 = db["channel_details"]

    for comt_data in coll1.find({},{"_id":0,"comment_info":1}):
        for i in range(len(comt_data["comment_info"])):
            cmt_list.append(comt_data["comment_info"][i])
    cot_df_table = st.dataframe(cmt_list)
    return cot_df_table

#streamlit code---->

with st.sidebar:
    st.title(":green[Youtube Data Harvesting and Warehousing]")
    st.header(":blue[**Skill Take Away**]")
    st.markdown(":black[Python Scripting]")
    st.markdown(":black[API intergration]")
    st.markdown(":black[Data Collection]")
    st.markdown(":black[MongoDB]")
    st.markdown(":gray[Data managament using MongoDB and SQL]")

channel_id = st.text_input("Enter the Channel ID")
channels = channel_id.split(',')
channels = [ch.strip() for ch in channels if ch]

if st.button("Collect and Store Data"):
    for channel in channels:
        ch_ids=[]
        db = client["youtube_project"]
        coll1 = db["channel_details"]
        for ch_data in coll1.find({},{"_id":0,"channel_info":1}):
            ch_ids.append(ch_data["channel_info"]['ch_id'])
        if channel in ch_ids:
            st.success("Channel ID already exists")
        else:
            insert = channel_details(channel)
            st.write("Scuccessfully inserted")


if st.button("Migrate to SQL"):
    Table = tables()
    st.success(Table)

show_table = st.radio("**Select the Table for View**",("Please select below","Channels","Playlist","Videos","Comments"))

if show_table == "Channels":
    show_channel_table()
elif show_table == "Playlist":
    show_playlist_table()
elif show_table == "Videos":
    show_video_table()
elif show_table == "Comments":
    show_comment_table()


#sql question connection---> st 
mydb = psycopg2.connect( host = "localhost",
                        user = "postgres",
                        password = "Mj@2590",
                        database = "youtube_data",
                        port = 5432)
cursor = mydb.cursor()

question = st.selectbox("**Select your question**",
                        ("Select the Question below  ",
                        "1. What are the names of all the videos and their corresponding channels?",
                        "2. Which channels have the most number of videos, and how many videos do they have?",
                        "3. What are the top 10 most viewed videos and their respective channels?",
                        "4. How many comments were made on each video, and what are their corresponding video names?",
                        "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
                        "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                        "7. What is the total number of views for each channel, and what are their corresponding channel names?",
                        "8. What are the names of all the channels that have published videos in the year 2022?",
                        "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                        "10.Which videos have the highest number of comments, and what are their corresponding channel names?"                        
                        ))


if question =="1. What are the names of all the videos and their corresponding channels?":
    q1 = '''select vi_title as videos, ch_name as channelname from videos'''
    cursor.execute(q1)
    mydb.commit()
    t1 = cursor.fetchall()
    df1 = pd.DataFrame(t1,columns=["Video Tilte","Channel Name"])
    st.write(df1)

elif question =="2. Which channels have the most number of videos, and how many videos do they have?":
    q2 = '''select ch_id as channelname, ch_videocount as no_videos from channels
            order by ch_videocount desc '''
    mydb.rollback()
    cursor.execute(q2)
    mydb.commit()
    t2 = cursor.fetchall()
    df2 = pd.DataFrame(t2,columns=["Channel Name","No of Videos",])
    st.write(df2)

elif question =="3. What are the top 10 most viewed videos and their respective channels?":
        q3 = '''select vi_viewcount as views, ch_name as channelname, vi_title as videotitle from videos
                where vi_viewcount is not null order by views desc limit 10'''
        mydb.rollback()
        cursor.execute(q3)
        mydb.commit()
        t3 = cursor.fetchall()
        df3 = pd.DataFrame(t3,columns=["Views","Channel Name","Videos Title"])
        st.write(df3)

elif question =="4. How many comments were made on each video, and what are their corresponding video names?":
        q4 = '''select vi_cmt_count as no_comments, vi_title as videotitle from videos  where vi_cmt_count is not null'''
        mydb.rollback()
        cursor.execute(q4)
        mydb.commit()
        t4 = cursor.fetchall()
        df4 = pd.DataFrame(t4,columns=["No of Comments","Videos Title"])
        st.write(df4)

elif question =="5. Which videos have the highest number of likes, and what are their corresponding channel names?":
        q5 = '''select  ch_name as channelname, vi_title as videotitle, vi_likecount as likes from videos 
                where vi_likecount is not null order by vi_likecount desc limit 20'''
        mydb.rollback()
        cursor.execute(q5)
        mydb.commit()
        t5 = cursor.fetchall()
        df5 = pd.DataFrame(t5,columns=["Channel Name","Video Title","No of Likes"])
        st.write(df5)

elif question =="6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
        q6 = '''select  vi_title as videotitle, vi_likecount as likes from videos 
                where vi_likecount is not null'''
        mydb.rollback()
        cursor.execute(q6)
        mydb.commit()
        t6 = cursor.fetchall()
        df6 = pd.DataFrame(t6,columns=["Video Title","No of Likes"])
        st.write(df6)

elif question =="7. What is the total number of views for each channel, and what are their corresponding channel names?":
        q7 = '''select  ch_id as channelname, ch_viewcount as viewscount from channels
                where ch_viewcount is not null'''
        mydb.rollback()
        cursor.execute(q7)
        mydb.commit()
        t7 = cursor.fetchall()
        df7 = pd.DataFrame(t7,columns=["Channel Name","Total No of Views"])
        st.write(df7)

elif question =="8. What are the names of all the channels that have published videos in the year 2022?":
        q8 = '''select  ch_name as channelname, vi_title as videotitle, vi_published as videopublished from videos
                where extract(year from vi_published)=2022'''
        mydb.rollback()
        cursor.execute(q8)
        mydb.commit()
        t8 = cursor.fetchall()
        df8 = pd.DataFrame(t8,columns=["Channel Name","Video Title","Video_Published"])
        st.write(df8)

elif question =="9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
        q9 = '''select  ch_name as channelname, avg(vi_duration) as averageduration from videos group by ch_name '''
        mydb.rollback()
        cursor.execute(q9)
        mydb.commit()
        t9 = cursor.fetchall()
        df9 = pd.DataFrame(t9,columns=["Channel_Name","Average_Duration"])

        T9=[]
        for index, row in df9.iterrows():
            channel_title = row['Channel_Name']
            average_duration = row['Average_Duration']
            average_duration_str = str(average_duration)
            T9.append({"Channel Name": channel_title ,  "Average Duration": average_duration_str})
        dd = pd.DataFrame(T9)
        st.write(dd)


elif question =="10.Which videos have the highest number of comments, and what are their corresponding channel names?":
        q10 = '''select  vi_title as videotitle, ch_name as channelname, vi_cmt_count as commentcounts from videos 
                where vi_cmt_count is not null order by vi_cmt_count desc '''
        mydb.rollback()
        cursor.execute(q10)
        mydb.commit()
        t10 = cursor.fetchall()
        df10 = pd.DataFrame(t10,columns=["Video title","Channel Name","Comment count"])
        st.write(df10)