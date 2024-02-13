from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st


#API Key Connection

api_key = "AIzaSyBmitvqDZm_7j5orupNb-UYv5H9SpaGpFs"
api_service_name="youtube"
api_version="v3"
youtube=build(api_service_name, api_version,developerKey=api_key)

# Function to retrieve details about a YouTube channel


def Channel_Details(Channel_Id):
    request =youtube.channels().list(
        part = "snippet,ContentDetails,statistics",
        id = Channel_Id
    )
    response = request.execute()

# Constructing request to get channel information


    for i in response['items']:
      info=dict(Channel_name=i['snippet']['title'],
                Channel_Description=i['snippet']['description'],
                Channel_Id=i['id'],
                Subscribers_Count=i['statistics']['subscriberCount'],
                Number_of_Views=i['statistics']['viewCount'],
                Total_videos=i['statistics']['videoCount'],
                Playlist_id=i['contentDetails']['relatedPlaylists']['uploads']
                )
    return info
    




#get video ids

def channel_video_id(Channel_id):
    video_ids=[]
    response=youtube.channels().list(id=Channel_id,
                                    part ="contentDetails").execute()
    Playlist_id= response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    Next_page_token=None
    while True:
                    response=youtube.playlistItems().list(
                                                    part="snippet",
                                                    playlistId=Playlist_id,
                                                    maxResults=50,
                                                    pageToken=Next_page_token).execute()
                    for i in range(len(response['items'])):
                        video_ids.append(response['items'][i]['snippet']['resourceId']['videoId'])

                    Next_page_token=response.get('nextPageToken')
                    if Next_page_token is None:
                      break
    return video_ids
Video_Ids=channel_video_id("UCIy4Q4CB_ppkrUwpK8Gu0zg")




#get video information

def get_video_details(Video_Ids):
    Video_info=[]
    for vd in Video_Ids:
        request=youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=vd
        )
        response=request.execute()
        for i in response['items']:
            info=dict(Channel_id=i['snippet']['channelId'],
                    Channel_Name=i['snippet']['channelTitle'],
                    Video_id=i.get('id'),
                    Published_date=i['snippet']['publishedAt'],
                    Title=i['snippet']['title'],
                    Comments=i['statistics'].get('commentCount'),
                    Duration=i['contentDetails']['duration'],
                    likes = i['statistics'].get('likeCount'),
                    dislikes = i['statistics'].get('dislikeCount'),
                    View_count=i['statistics']['viewCount'])
            Video_info.append(info)
    return Video_info




#get comment data

def get_comment_details(video_idd):
    comment_info=[]
    try:
        for vdd in video_idd:
                request=youtube.commentThreads().list(
                    part="snippet",
                    videoId=vdd,
                    maxResults=50
                )
                response=request.execute()
                for i in response['items']:
                    data=dict(CommentId=i['snippet']['topLevelComment']['id'],
                            videoid=i['snippet']['topLevelComment']['snippet']['videoId'],
                            comment_text=i['snippet']['topLevelComment']['snippet'].get('textDisplay'),
                            comment_author=i['snippet']['topLevelComment']['snippet']['authorDisplayName']
                            )
                    comment_info.append(data)

    except:
     pass
    return comment_info

client = pymongo.MongoClient("localhost",27017)
db = client["youtubeData"]
collection = db["Channel_Details"]
def get_Channel_Details(Channel_Id):
   Ch_details=Channel_Details(Channel_Id)
   vi_ids= channel_video_id(Channel_Id)
   Vi_info= get_video_details(vi_ids)
   com_details=get_comment_details(vi_ids)
   collection.insert_one({"Channel_information":Ch_details,"Video_information":Vi_info,"Comment_information":com_details})



#Table creation for channels

def channel_table():
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="Muthur",
                        database="youtubedata_scrapping",
                        port="5432")
    cursor=mydb.cursor()

    drop_query=''' drop table if exists channels'''
    cursor.execute(drop_query)
    mydb.commit()

    try:
        query_for_table='''create table channels(Channel_name varchar(100),
                                                            Channel_Description varchar(250),
                                                            Channel_Id varchar(100) primary key,
                                                            Subscribers_Count int,
                                                            Number_of_Views bigint,
                                                            Total_videos int,
                                                            Playlist_id varchar(100))'''
        cursor.execute(query_for_table)
        mydb.commit()
        print("table 'channels' created successfuly")
        
        
    except psycopg2.Error as e:
        print("Error occurred while working with PostgreSQL:", e)
channel_table()


#table creation for video


def video_table():
        
                mydb = psycopg2.connect(host="localhost",
                                        user="postgres",
                                        password="Muthur",
                                        database="youtubedata_scrapping",
                                        port="5432")
                cursor = mydb.cursor()

                cursor.execute('''DROP TABLE IF EXISTS videos''')
                mydb.commit()


                query_for_table = '''CREATE TABLE videos (
                                        Channel_id varchar(50),
                                        Channel_Name varchar(50),
                                        Video_idd varchar(30) PRIMARY KEY,
                                        Published_date TIMESTAMP,
                                        Title varchar(250),
                                        Comments BIGINT,
                                        Duration INTERVAL,
                                        likes BIGINT,
                                        dislikes BIGINT,
                                        View_count BIGINT
                                    )'''
                cursor.execute(query_for_table)
                mydb.commit()
                print("Table 'videos' created successfully")

video_table()


#table for comments

# Function to create the "comments" table
def create_comments_table():
    try:
        mydb = psycopg2.connect(host="localhost",
                                user="postgres",
                                password="Muthur",
                                database="youtubedata_scrapping",
                                port="5432")
        cursor = mydb.cursor()

        cursor.execute('''DROP TABLE IF EXISTS comments''')
        mydb.commit()

        query_for_table = '''CREATE TABLE comments (
                                CommentId VARCHAR(50) PRIMARY KEY,
                                videoid VARCHAR(30),
                                comment_text TEXT,
                                comment_author VARCHAR(150)
                            )'''
        cursor.execute(query_for_table)
        mydb.commit()
        print("Table 'comments' created successfully")
    except psycopg2.Error as e:
        print("Error occurred while working with PostgreSQL:", e)
    finally:
        cursor.close()
        mydb.close()

# Call the function to create the "comments" table
create_comments_table()


#code for streamlit overall info


st.title(':red[YOUTUBE DATA HARVESTING AND WAREHOUSING]')

# Homepage content
st.header('Project Description')
st.write('Welcome to the YouTube Data Migration project!')
st.write('This app allows you to fetch data from a YouTube channel, store it in MongoDB, and migrate it to PostgreSQL.')

# Sidebar with project info
with st.sidebar:
    st.title('About')
    st.info('This app retrieves YouTube channel data using the YouTube API.')


#get the input from user
channel_id = st.text_input("Enter your Channel Id")

#Extract data
if st.button("Extract data"):
  if channel_id:
            info = Channel_Details(channel_id)
            Video_info=get_video_details(Video_Ids)
            comment_info=get_comment_details(Video_Ids)
            st.success("Data Extracted Successfully")
  else:
            st.warning("Please enter a YouTube Channel ID.")

            

# Function to connect to MongoDB
def get_data_and_upload_to_mongodb(channel_id):
      existing_document = collection.find_one({"Channel_information.Channel_Id": channel_id})
      if existing_document:
           st.warning("The given channel details already exists")
      else:
           get_Channel_Details(channel_id)
           st.success(f"Data for Channel ID {channel_id} Uploaded to MongoDB Successfully.")
if st.button("Upload Data to MongoDB"):
    if channel_id:
        get_data_and_upload_to_mongodb(channel_id)
    else:
        st.warning("Please enter a YouTube Channel ID.")

def fetch_channel_names():
    channel_names = []
    client = pymongo.MongoClient("localhost", 27017)
    db = client["youtubeData"]
    collection = db["Channel_Details"]
    
    distinct_channel_names = collection.distinct("Channel_information.Channel_name")
    channel_names.extend(distinct_channel_names)
    
    return channel_names


# Fetch distinct channel names
channel_names = fetch_channel_names()


# Function to fetch channel, video, and comment details from MongoDB based on selected channel
def fetch_channel_video_comment_details(selected_channel_name):
    client = pymongo.MongoClient("localhost", 27017)
    db = client["youtubeData"]
    collection = db["Channel_Details"]

    # Fetch the document containing channel, video, and comment information
    selected_channel = collection.find_one({"Channel_information.Channel_name": selected_channel_name})

    # Extract channel, video, and comment details from the selected channel data
    channel_info = selected_channel.get("Channel_information", {})
    video_info = selected_channel.get("Video_information", [])
    comment_info = selected_channel.get("Comment_information", [])

    return channel_info, video_info, comment_info


# Retrieve selected channel name from the user
selected_channel_name = st.selectbox("Select a Channel to insert data into PostgreSQL", channel_names, key='selectbox')

# Fetch channel, video, and comment details based on selected channel
channel_data, video_data, comment_data = fetch_channel_video_comment_details(selected_channel_name)

# Establish connection to PostgreSQL
mydb = psycopg2.connect(
    host="localhost",
    user="postgres",
    password="Muthur",
    database="youtubedata_scrapping",
    port="5432"
)
cursor = mydb.cursor()

# Button for data insertion
button_clicked = st.button("Insert Data into PostgreSQL")

if button_clicked:
    if channel_data:
        # Insert channel details into PostgreSQL 'channels' table
        insert_channel_data = '''
            INSERT INTO channels (
                Channel_name,
                Channel_Description,
                Channel_Id,
                Subscribers_Count,
                Number_of_Views,
                Total_videos,
                Playlist_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        '''
        channel_values = (
            channel_data.get("Channel_name", ""),
            channel_data.get("Channel_Description", ""),
            channel_data.get("Channel_Id", ""),
            channel_data.get("Subscribers_Count", ""),
            channel_data.get("Number_of_Views", ""),
            channel_data.get("Total_videos", ""),
            channel_data.get("Playlist_id", "")
        )
        cursor.execute(insert_channel_data, channel_values)

    if video_data:
        # Insert video details into PostgreSQL 'videos' table
        insert_video_data = '''
            INSERT INTO videos (
                Channel_id,
                Channel_Name,
                Video_idd,
                Published_date,
                Title,
                Comments,
                Duration,
                likes,
                dislikes,
                View_count
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''
        for video in video_data:
            video_values = (
                video.get("Channel_id", ""),
                video.get("Channel_Name", ""),
                video.get("Video_idd", ""),
                video.get("Published_date", ""),
                video.get("Title", ""),
                video.get("Comments", ""),
                video.get("Duration", ""),
                video.get("likes", ""),
                video.get("dislikes", ""),
                video.get("View_count", "")
            )
            cursor.execute(insert_video_data, video_values)

    if comment_data:
        # Insert comment details into PostgreSQL 'comments' table
        insert_comment_data = '''
            INSERT INTO comments (
                CommentId,
                videoid,
                comment_text,
                comment_author
            ) VALUES (%s, %s, %s, %s)
        '''
        for comment in comment_data:
            comment_values = (
                comment.get("CommentId", ""),
                comment.get("videoid", ""),
                comment.get("comment_text", ""),
                comment.get("comment_author", "")
            )
            cursor.execute(insert_comment_data, comment_values)

    mydb.commit()
    st.success(f"Data inserted successfully for channel: {selected_channel_name}")

check_query = "SELECT * FROM channels WHERE Channel_name = %s"  # Assuming 'Channel_name' is the unique identifier

# Assuming you have channel_values ready for insertion
cursor.execute(check_query, (channel_data.get("Channel_name", ""),))
existing_data = cursor.fetchone()

if existing_data:
  st.warning("Data already exists for this channel in PostgreSQL.")


#view part
mydb = psycopg2.connect(host="localhost",
                    user="postgres",
                    password="Muthur",
                    database="youtubedata_scrapping",
                    port="5432")
cursor = mydb.cursor()

query=st.selectbox("select your Database Query",("1.What are the names of all the videos and their corresponding channels?",
                                            "2.Which channels have the most number of videos, and how many videos do they have?",
                                            "3.What are the top 10 most viewed videos and their respective channels?", 
                                            "4.How many comments were made on each video, and what are their corresponding video names?",
                                            "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
                                            "6.What is the total number of likes and dislikes for each video, and what are their corresponding videonames?",
                                            "7.What is the total number of views for each channel, and what are their corresponding channel names?",
                                            "8.What are the names of all the channels that have published videos in the year 2022?",
                                            "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                            "10.Which videos have the highest number of comments, and what are their corresponding channel names:?"
                                            ))
if query=="1.What are the names of all the videos and their corresponding channels?":

   query1='''select Title as title, Channel_Name as channelname from videos '''
   cursor.execute(query1)
   mydb.commit()
   t1=cursor.fetchall()
   df=pd.DataFrame(t1,columns=["video title","channel name"])
   st.write(df)

elif query=="2.Which channels have the most number of videos, and how many videos do they have?":
   query2='''select Channel_name as channelname, Total_videos as No_of_videos from channels order by Total_videos desc '''
   cursor.execute(query2)
   mydb.commit()
   t2=cursor.fetchall()
   df2=pd.DataFrame(t2,columns=["Channel Name","Number of Videos"])
   st.write(df2)

elif query == "3.What are the top 10 most viewed videos and their respective channels?":
  
   query3='''select View_count as views, Channel_Name as channel name, Title as title from videos where View_count is not null order by View_count desc limit 10'''
   cursor.execute(query3)
   mydb.commit()
   t3=cursor.fetchall()
   df3=pd.DataFrame(t3,columns=["Views","Channel Name","Video Title"])
   st.write(df3)

elif query== "4.How many comments were made on each video, and what are their corresponding video names?":
   query4='''select Comments as comments, Title as title from videos where Comments is not null'''
   cursor.execute(query4)
   mydb.commit()
   t4=cursor.fetchall()
   df4=pd.DataFrame(t4,columns=["No_of_comments","Video Title"])
   st.write(df4)

elif query == "5.Which videos have the highest number of likes, and what are their corresponding channel names?":
   query5='''select Channel_Name as channel name, Title as title,likes as likes from videos where likes is not null order by likes desc'''
   cursor.execute(query5)
   mydb.commit()
   t5=cursor.fetchall()
   df5=pd.DataFrame(t5,columns=["Channelname","Video Title","like_count"])
   st.write(df5)

elif query == "6.What is the total number of likes and dislikes for each video, and what are their corresponding videonames?":
   query6='''select dislikes as dislikes, Title as title,likes as likes from videos'''
   cursor.execute(query6)
   mydb.commit()
   t6=cursor.fetchall()
   df6=pd.DataFrame(t6,columns=["Dislikes","Video Title","like_count"])
   st.write(df6)

elif query =="7.What is the total number of views for each channel, and what are their corresponding channel names?":
   query7='''select Channel_name as channel name, Number_of_Views as views from channels'''
   cursor.execute(query7)
   mydb.commit()
   t7=cursor.fetchall()
   df7=pd.DataFrame(t7,columns=["Channel Name","Number of views"])
   st.write(df7)

elif query ==  "8.What are the names of all the channels that have published videos in the year 2022?":
   query8='''select Title as title, Published_date as releasedate, Channel_Name as channel name from videos where extract(year from Published_date)=2022'''
   cursor.execute(query8)
   mydb.commit()
   t8=cursor.fetchall()
   df8=pd.DataFrame(t8,columns=["Video title","published date","channelname"])
   st.write(df8)

elif query =="9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":
   query9='''select Channel_Name as channel name, AVG(Duration) as duration, from videos group by channel_Name'''
   cursor.execute(query9)
   mydb.commit()
   t9=cursor.fetchall()
   df9=pd.DataFrame(t9,columns=["channel name","Average duration"])
   
   T9=[]
   for index,row in df9.iterrows():
       channel_title=row["Channel_Name"]
       avg_duration=row["Duration"]
       avg_duration_str=str(avg_duration)
       T9.append(dict(channeltitle=channel_title,avgdurtion=avg_duration_str))
       df1=pd.DataFrame(T9)
       st.write(df1)

elif query == "10.Which videos have the highest number of comments, and what are their corresponding channel names:?":
   query10='''select Channel_Name as channel name, Title as title, Comments as comments from videos where Comments is not null order by Comments desc'''
   cursor.execute(query10)
   mydb.commit()
   t10=cursor.fetchall()
   df10=pd.DataFrame(t10,columns=["Channel Name","Video Title","No_of-Comments"])
   st.write(df10)

   











 























        
                                        












