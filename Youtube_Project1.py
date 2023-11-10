from googleapiclient.discovery import build
import pymongo
import pyodbc
import mysql.connector
import pandas as pd
import streamlit as st

client=pymongo.MongoClient("mongodb://localhost:27017/")
db=client["Project_1"]

# Connect SQL DB and Create Tables

host="localhost"
user="root"
password="Jeswin@123"
database="Project_1"

mydb=mysql.connector.connect(
    host=host,
    user=user,
    password=password,
    database=database
)
cursor = mydb.cursor()

# API Key Connection
def api_connect():
    api_id="AIzaSyAYiyS37IVlN19w5xIe8LbDehH3ByMHG9g"

    api_service_name = "youtube"
    api_version="v3"

    youtube=build(api_service_name, api_version, developerKey=api_id)

    return youtube

youtube = api_connect()

# Get Channel Information

def get_channel_info(channel_id):
    request = youtube.channels().list(
                  part="snippet, contentDetails, statistics",
                  id=channel_id)
    
    response = request.execute()

    for i in response['items']:
        data = dict(Channel_Name=i ['snippet']['title'],
                Channel_Id=i ['id'],
                Subscribers=i ['statistics']['subscriberCount'],
                Views=i ['statistics']['viewCount'],
                Total_Videos=i ['statistics']['videoCount'],
                Channel_Description=i ['snippet']['description'],
                Playlist_Id=i ['contentDetails']['relatedPlaylists']['uploads'])
        return data

# Get Playlist Details

def get_playlist_details(channal_id):
        next_page_token=None
        All_data=[]
        while True:
                request=youtube.playlists().list(
                    part='snippet,contentDetails',
                    channelId=channal_id,
                    maxResults=50,
                    pageToken=next_page_token
                )
                
                response=request.execute()
                for item in response['items']:
                    data=dict(Playlist_Id=item['id'],
                            Title=item['snippet']['title'],
                            Channel_Id=item['snippet']['channelId'],
                            Channel_Name=item['snippet']['channelId'],
                            PulishedAt=item['snippet']['publishedAt'],
                            Video_Count=item['contentDetails']['itemCount'])
                    All_data.append(data)
                next_page_token=response.get('nextPageToken')
                if next_page_token is None:
                        break
        return All_data
    
# Get Playlist IDs
def get_videos_ids(channel_id):
    video_ids = []
    response = youtube.channels().list(id=channel_id, part='contentDetails').execute()
    Playlist_Id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None

    while True:
        response1 = youtube.playlistItems().list(
            part='snippet',
            playlistId=Playlist_Id,
            maxResults=50, 
            pageToken=next_page_token).execute()
        
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])  
        next_page_token = response1.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids

# Get Video Information

def get_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
        request=youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id 
        )
        response=request.execute()

        for item in response["items"]:
            data=dict(Channel_Name=item['snippet']['channelTitle'],
                    Channel_Id=item['snippet']['channelId'],
                    Video_Id=item['id'],
                    Title=item['snippet']['title'],
                    Tags=item['snippet'].get('tags'),
                    Thumbnail=item['snippet']['thumbnails']['default']['url'],
                    Description=item['snippet'].get('description'),
                    Published_Date=item['snippet']['publishedAt'],
                    Duration=item['contentDetails']['duration'],
                    Views=item['statistics'].get('viewCount'),
                    Likes=item['statistics'].get('likeCount'),
                    Comments=item['statistics'].get('commentCount'),
                    Favourate_Count=item['statistics']['favoriteCount'],
                    Defition=item['contentDetails']['definition'],
                    Catption_Status=item['contentDetails']['caption'])
                    
            video_data.append(data)
    return video_data

# Get Video Comment Informations

def get_comment_info(video_ids):
    Comment_data=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                part='snippet',
                videoId=video_id,
                maxResults=50,
            )
            response=request.execute()

            for item in response['items']:
                data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                        Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])

                Comment_data.append(data)
    except:
        pass
    return Comment_data

# Insert Video Data to MongoDB 

# 10 Youtube Channel Details

## 1.  69 Videos  - Sparrow Trader -         Channel ID:  'UCUoWLVyIdUmIb65dwmqeFsw'
## 2.  85 Videos  - Ben Armstrong -          Channel ID:  'UCuV9EB4I9L-xmRoaXd8tmuA'
## 3.  147 Videos - Official Bazaar -        Channel ID:  'UCtvnbhsV33WxHBYOB4jaaFw'
## 4.  179 Videos - Cric Star v1 -           Channel ID:  'UC80Dc7dpmMslkVSy0zeqA4A'
## 5.  223 Videos - SRVTWIZ Tamil -          Channel ID:  'UC2juvXALxuiVx_2gkGBy1mA'
## 6.  564 Videos - Sahiba Makeover -        Channel ID:  'UC920YFSo4MV0KMNdrm7ushQ'
## 7.  171 Videos - Crypto Pugal -           Channel ID:  'UCk1ZZD4f6PxZz-SAvlRdfnw'
## 8.  403 Videos - Dr. Sanjay Tolani -      Channel ID:  'UCBf1VE1YRhg8DQwO4krwCiw'
## 9.  128 Videos - DAY TRADER తెలుగు 2.0    Channel ID:   'UC0jB9fB5gzCplA3-2pGw6ag'
## 10. 414 Videos - Bible Wisdom Tamil       Channel ID:   'UCxYX8yjop07BZG2We4jdVLg'


# Mongo DB Connection
client=pymongo.MongoClient("mongodb://localhost:27017/")
db=client["Project_1"]


# Data Upload to MongoDB

def YoutubeData(channel_id):
    chl_details=get_channel_info(channel_id)
    ply_details=get_playlist_details(channel_id)
    vdo_ids=get_videos_ids(channel_id)
    vdo_details=get_video_info(vdo_ids)
    com_details=get_comment_info(vdo_ids)

    Project_1=db["YoutubeData"]
    Project_1.insert_one({"channel_info":chl_details, "playlist_info":ply_details,
                            "video_info":vdo_details, "comment_info":com_details})

    return "upload completed successfully"


# Table Creation for Channels, Playlist, Videos and Comments
def channels_table():
    
    # Connecting MySQL DB
    mydb=mysql.connector.connect(
    host=host,
    user=user,
    password=password,
    database=database
    )
    cursor = mydb.cursor()
    
    drop_query = "drop table if exists channels"
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query='''create table if not exists channels(Channel_Name varchar(255),
                                                        Channel_Id varchar(90) primary key,
                                                        Subscribers bigint,
                                                        Views bigint,
                                                        Total_Videos int,
                                                        Channel_Description text,
                                                        Playlist_Id varchar(90))'''
        cursor.execute(create_query)
        mydb.commit()
    except:
        st.write("Channels Table alredy created")

    
    # Pull / Fetch Data from MongoDB for "Channels" Table
    ch_list=[]
    db=client["Project_1"]
    coll1=db["YoutubeData"]
    for ch_data in coll1.find({},{"_id":0, "channel_info":1}):
        ch_list.append(ch_data['channel_info'])
    df=pd.DataFrame(ch_list)


    for index, row in df.iterrows():
        insert_sqlquery='''insert into channels(Channel_Name,
                                                Channel_Id,
                                                Subscribers,
                                                Views,
                                                Total_Videos,
                                                Channel_Description,
                                                Playlist_Id)
                                                
                                                values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['Channel_Name'],
                row['Channel_Id'],
                row['Subscribers'],
                row['Views'],
                row['Total_Videos'],
                row['Channel_Description'],
                row['Playlist_Id'])
        try:
            cursor.execute(insert_sqlquery,values)
            mydb.commit()
        except:
            st.write("Channels values already inserted")

def playlist_table():
        
        # Connecting MySQL DB
        mydb=mysql.connector.connect(
                host=host,
                user=user,
                password=password,
                database=database)
        cursor = mydb.cursor()

        # Drop table if exists "Playlists"
        drop_query='''drop table if exists playlists'''
        cursor.execute(drop_query)
        mydb.commit()
                
        from datetime import datetime
        original_date = '2021-11-29T08:48:22Z'
        formatted_date = datetime.strptime(original_date, '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')

        # Creating Table "Playlists"
        try:
            create_query='''create table if not exists playlists(Playlist_Id varchar(255) primary key,
                                                             Title varchar(100),
                                                             Channel_Id varchar(100),
                                                             Channel_Name varchar(100),
                                                             PulishedAt timestamp,
                                                             Video_Count int
                                                             )'''
            cursor.execute(create_query)
            mydb.commit()
        except:
            st.write("Playlists Table already created")

        # Pull / Fetch Data from MongoDB for "Playlists" Table
        pl_list=[]
        db=client["Project_1"]
        coll1=db["YoutubeData"]
        for pl_data in coll1.find({},{"_id":0, "playlist_info":1}):
            for i in range(len(pl_data["playlist_info"])):
                pl_list.append(pl_data["playlist_info"][i])
        df1=pd.DataFrame(pl_list)


        # Insert data into MySQL table "Playlists"
        for index, row in df1.iterrows():
                title = row['Title'][:255]  # Truncate 'Title' to 255 characters
                original_date = row['PulishedAt']
                formatted_date = datetime.strptime(original_date, '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
                
                insert_sqlquery='''insert into playlists(Playlist_Id,
                                                        Title,
                                                        Channel_Id,
                                                        Channel_Name,
                                                        PulishedAt,
                                                        Video_Count)
                                                        
                                                        values(%s,%s,%s,%s,%s,%s)'''
                    
                values=(row['Playlist_Id'],
                        title,
                        row['Channel_Id'],
                        row['Channel_Name'],
                        formatted_date,
                        row['Video_Count'])
                try:
                    cursor.execute(insert_sqlquery,values)
                    mydb.commit()
                except:
                    st.write("Playlist values already inserted")

def videos_table():

    # Connecting MySQL DB
    mydb=mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database)
    cursor = mydb.cursor()

    # Drop table if exists "Videos"
    drop_query='''drop table if exists videos'''
    cursor.execute(drop_query)
    mydb.commit()


    # Creating Table "Videos"
    try:
        create_query='''create table if not exists videos(Channel_Name varchar(100),
                                                Channel_Id varchar(100),
                                                Video_Id varchar(40) primary key,
                                                Title varchar(255),
                                                Tags text,
                                                Thumbnail varchar(200),
                                                Description text,
                                                Published_Date  timestamp,
                                                Duration time,
                                                Views bigint,
                                                Likes bigint,
                                                Comments int,
                                                Favourate_Count int,
                                                Defition varchar(20),
                                                Catption_Status varchar(100)
                                                        )'''
        cursor.execute(create_query)
        mydb.commit()
    except:
        st.write("Videos Table already created")        

    # Pull / Fetch Data from MongoDB for "Videos" Table
    vi_list=[]
    db=client["Project_1"]
    coll1=db["YoutubeData"]
    for vi_data in coll1.find({},{"_id":0, "video_info":1}):
        for i in range(len(vi_data["video_info"])):
            vi_list.append(vi_data["video_info"][i])
    df2=pd.DataFrame(vi_list)

    # Insert data into MySQL table "Videos"

    from datetime import datetime
    import re

    for index, row in df2.iterrows():
        # Convert ISO 8601 format to MySQL timestamp format
        published_date = datetime.strptime(row['Published_Date'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')

    # Convert 'PT7M36S' to MySQL time format (HH:MM:SS)
    duration_str = row['Duration']
    match = re.match(r'PT(\d+)M(\d+)S', duration_str)
    if match:
        minutes, seconds = map(int, match.groups())
        duration_mysql = '{:02}:{:02}:00'.format(minutes, seconds)
    else:
        duration_mysql = '00:00:00'

    tags = row['Tags']
    if not isinstance(tags, list):
        tags = [tags]  if tags is not None else[] # If 'Tags' is not a list, convert it to a single-item list

    # Filter out None values and join the tags as a comma-separated string
    tags_str = ','.join(filter(None, tags))

    insert_sqlquery = '''insert into videos(Channel_Name,
                                            Channel_Id,
                                            Video_Id,
                                            Title,
                                            Tags,
                                            Thumbnail,
                                            Description,
                                            Published_Date,
                                            Duration,
                                            Views,
                                            Likes,
                                            Comments,
                                            Favourate_Count,
                                            Defition,
                                            Catption_Status)
                                            
                                            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''

    values = (row['Channel_Name'],
                row['Channel_Id'],
                row['Video_Id'],
                row['Title'],
                tags_str,
                row['Thumbnail'],
                row['Description'],
                published_date,
                duration_mysql,
                row['Views'],
                row['Likes'],
                row['Comments'],
                row['Favourate_Count'],
                row['Defition'],
                row['Catption_Status'])
    try:
        cursor.execute(insert_sqlquery, values)
        mydb.commit()
    except:
        st.write("Video values already inserted in to table")

        
def comments_table():
        
        # Connecting MySQL DB
        mydb=mysql.connector.connect(
                host=host,
                user=user,
                password=password,
                database=database)
        cursor = mydb.cursor()

        # Drop table if exists "Playlists"
        drop_query='''drop table if exists comments'''
        cursor.execute(drop_query)
        mydb.commit()

        # Creating Table "Playlists"
        try:
            create_query='''create table if not exists comments(Comment_Id varchar(100),
                                                            Video_Id varchar(60),
                                                            Comment_Text text,
                                                            Comment_Author varchar(150),
                                                            Comment_Published timestamp
                                                             )'''
          
                                        
            cursor.execute(create_query)
            mydb.commit()
        
        except:
            st.write("Comments Table already created")

        # Pull / Fetch Data from MongoDB for "Playlists" Table
        com_list=[]
        db=client["Project_1"]
        coll1=db["YoutubeData"]
        for com_data in coll1.find({},{"_id":0, "comment_info":1}):
                for i in range(len(com_data["comment_info"])):
                        com_list.append(com_data["comment_info"][i])
        df3=pd.DataFrame(com_list)


        # Insert data into MySQL table "Comment"

        from datetime import datetime                                            
        
        for index, row in df3.iterrows():
                comment_published = datetime.strptime(row['Comment_Published'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
       
        insert_sqlquery='''insert into comments(Comment_Id,
                                                Video_Id,
                                                Comment_Text,
                                                Comment_Author,
                                                Comment_Published)
                                                
                                                values(%s,%s,%s,%s,%s)'''
                                                     
        values=(row['Comment_Id'],
                row['Video_Id'],
                row['Comment_Text'],
                row['Comment_Author'],
                comment_published)
        
        try:
            cursor.execute(insert_sqlquery,values)
            mydb.commit()
        except:
            st.write("This comments are already exist in comments table")

def table_create_insert():   ## Combine all functions into One Main function.
    channels_table()
    playlist_table()
    videos_table()    
    comments_table()

    return "Tables Created and Data Upload Successfully"

def view_channel_table():
    ch_list=[]
    db=client["Project_1"]
    coll1=db["YoutubeData"]
    for ch_data in coll1.find({},{"_id":0, "channel_info":1}):
        ch_list.append(ch_data['channel_info'])
    channel_table = df=st.dataframe(ch_list)
    return channels_table

def view_playlist_table():
    pl_list=[]
    db=client["Project_1"]
    coll1=db["YoutubeData"]
    for pl_data in coll1.find({},{"_id":0, "playlist_info":1}):
        for i in range(len(pl_data["playlist_info"])):
            pl_list.append(pl_data["playlist_info"][i])
    playlists_table = df1=st.dataframe(pl_list)
    return playlists_table

def view_video_table():
    vi_list=[]
    db=client["Project_1"]
    coll1=db["YoutubeData"]
    for vi_data in coll1.find({},{"_id":0, "video_info":1}):
        for i in range(len(vi_data["video_info"])):
            vi_list.append(vi_data["video_info"][i])
    videos_table = df2=st.dataframe(vi_list)
    return videos_table

def view_comment_table():
    com_list=[]
    db=client["Project_1"]
    coll1=db["YoutubeData"]
    for com_data in coll1.find({},{"_id":0, "comment_info":1}):
        for i in range(len(com_data["comment_info"])):
            com_list.append(com_data["comment_info"][i])
    comments_table = df3=st.dataframe(com_list)
    return comments_table

# Streamlit Part
with st.sidebar:
    st.title(":green[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("Skils Learned")
    st.caption("Python Coding")
    st.caption(" YouTube Data Collection")
    st.caption("MongoDB")
    st.caption("API Creation & Integration")
    st.caption("Data Feed and Migration from MongoDB to SQL")

channel_id = st.text_input("Enter Channel ID")
channels = channel_id.split(',')
channels = [ch.strip() for ch in channels if ch]

if st.button("Collect and Store Data"):
    for channel in channels:
        ch_ids = []
        db = client["Project_1"]
        coll1 = db["YoutubeData"]
        for ch_data in coll1.find({}, {"_id": 0, "channel_info": 1}):
            ch_ids.append(ch_data["channel_info"]["Channel_Id"])
        if channel_id in ch_ids:
            st.success("Channel details of the given channel id: "+ channel +" already exist")
        else:
            output=YoutubeData(channel)
            st.success(output)

if st.button("Migrat to SQL"):
    display=table_create_insert()
    st.success(display)

show_table = st.radio("SELECT THE TABLE FOR VIEW",(":green[channels]",":orange[playlists]",":red[videos]",":blue[comments]"))

if show_table == ":green[channels]":
    view_channel_table()
elif show_table == ":orange[playlists]":
    view_playlist_table()
elif show_table ==":red[videos]":
    view_video_table()
elif show_table == ":blue[comments]":
    view_comment_table()

# SQL Connection

mydb=mysql.connector.connect(
    host=host,
    user=user,
    password=password,
    database=database)
cursor = mydb.cursor()

question = st.selectbox(
                     'Select Your Query',
                     ('1. All the Videos and Channel Names',
                      '2. Channels with High Number of Videos',
                      '3. Top 10 viewed Videos and Channel Names',
                      '4. Total Number of Comment on Videos and Video Names',
                      '5. Videos with Highest Likes and Video Names',
                      '6. Total Likes and Dislikes and Video Names',
                      '7. Total Number of Channel Views and Channel Names',
                      '8. List of Channel Names for Videos Published in 2022',
                      '9. Average Duration of all Videls and Channel Names',
                      '10. Videos having Highest Number of Comments and Channel Name'))
    
if question == '1. All the Videos and Channel Names':
        query1 = "SELECT Title as videos, Channel_Name as ChannelName from videos;"
        cursor.execute(query1)
        t1=cursor.fetchall()
        st.write(pd.DataFrame(t1, columns=["Video Title","Channel Name"]))
        mydb.commit()

elif question == '2. Channels with High Number of Videos':
    query2 = "SELECT Channel_Name as ChannelName,Total_Videos as NO_Videos from channels order by Total_Videos desc;"
    cursor.execute(query2)
    t2=cursor.fetchall()
    st.write(pd.DataFrame(t2, columns=["Channel Name","No Of Videos"]))
    mydb.commit()

elif question == '3. Top 10 viewed Videos and Channel Names':
    query3 = '''SELECT Views as views , Channel_Name as ChannelName,Title as VideoTitle from videos 
                        where Views is not null order by Views desc limit 10;'''
    cursor.execute(query3)
    t3 = cursor.fetchall()
    st.write(pd.DataFrame(t3, columns = ["views","channel Name","video title"]))
    mydb.commit()

elif question == '4. Total Number of Comment on Videos and Video Names':
    query4 = "SELECT Comments as No_comments ,Title as VideoTitle from videos where Comments is not null;"
    cursor.execute(query4)
    t4=cursor.fetchall()
    st.write(pd.DataFrame(t4, columns=["No Of Comments", "Video Title"]))
    mydb.commit()

elif question == '5. Videos with Highest Likes and Video Names':
    query5 = '''SELECT Title as VideoTitle, Channel_Name as ChannelName, Likes as LikesCount from videos 
                       where Likes is not null order by Likes desc;'''
    cursor.execute(query5)
    t5 = cursor.fetchall()
    st.write(pd.DataFrame(t5, columns=["video Title","channel Name","like count"]))
    mydb.commit()

elif question == '6. Total Likes and Dislikes and Video Names':
    query6 = '''SELECT Likes as likeCount,Title as VideoTitle from videos;'''
    cursor.execute(query6)
    t6 = cursor.fetchall()
    st.write(pd.DataFrame(t6, columns=["like count","video title"]))
    mydb.commit()

elif question == '7. Total Number of Channel Views and Channel Names':
    query7 = "SELECT Channel_Name as ChannelName, Views as Channelviews from channels;"
    cursor.execute(query7)
    t7=cursor.fetchall()
    st.write(pd.DataFrame(t7, columns=["channel name","total views"]))
    mydb.commit()

elif question == '8. List of Channel Names for Videos Published in 2022':
    query8 = '''SELECT Title as Video_Title, Published_Date as VideoRelease, Channel_Name as ChannelName from videos 
                where extract(year from Published_Date) = 2022;'''
    cursor.execute(query8)
    t8=cursor.fetchall()
    st.write(pd.DataFrame(t8,columns=["Name", "Video Publised On", "ChannelName"]))
    mydb.commit()

elif question == '9. Average Duration of all Videls and Channel Names':
    query9 =  "SELECT Channel_Name as ChannelName, AVG(Duration) AS average_duration FROM videos GROUP BY Channel_Name;"
    cursor.execute(query9)
    t9=cursor.fetchall()
    t9 = pd.DataFrame(t9, columns=['ChannelTitle', 'Average Duration'])
    T9=[]
    for index, row in t9.iterrows():
        channel_title = row['ChannelTitle']
        average_duration = row['Average Duration']
        average_duration_str = str(average_duration)
        T9.append({"Channel Title": channel_title ,  "Average Duration": average_duration_str})
    st.write(pd.DataFrame(T9))
    mydb.commit()

elif question == '10. Videos having Highest Number of Comments and Channel Name':
    query10 = '''SELECT Title as VideoTitle, Channel_Name as ChannelName, Comments as Comments from videos 
                       where Comments is not null order by Comments desc;'''
    cursor.execute(query10)
    t10=cursor.fetchall()
    st.write(pd.DataFrame(t10, columns=['Video Title', 'Channel Name', 'No. Of Comments']))
    mydb.commit()