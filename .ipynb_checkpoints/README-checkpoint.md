# Introduction 
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
The role of a data engineer is to extract the data from S3. After that, processing the data via the etl pipeline using Spark. In order to help Sparkify's analytical team in finding useful insight the could help in making Sparkify a more advanced app,  loading the data into dimensional tables is going to be the next step after processing the data using Spark. All the previous steps will be done by building an ETL pipeline that could handle the procedure.

# Database schema 
The schema of the data based will be a star schema. it will consist of a one fact table and four dimensional tables.
Fact table: songplay.
Dimesional tables: users, songs, artist and time.

# ETL pipeline 
The process of the ETL pipeline will start by extracting the data from S3 for log data and songs data by two diffrent fuctions the first is going to be process_song_data where the song data will be processed by extracting then loading data into songs and artists tables. After that, process_song_data function will be used to extract log data then load it into time, songplays and users tables on S3.

# Repository files 
1. ETL.py: This file is the ETL pipeline where all the extracting and loading processes going to be done in this file. 
2. dl.cfg : this file contains AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY for IAM user where must be used in order to read the S3 files, load into S3. 


