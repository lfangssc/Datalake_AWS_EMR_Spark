# Datalake_AWS_EMR_Spark
Project: Build a datalake using AWS s3, EMR and Pyspark

Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. 

The goal of the project is to use the AWS Elastic MapReduce (EMR) and Pyspark SQL to load data from AWS S3 and wirte the data in fact/dimension talbes that can be efficiently queried for business purpose.

The workflow of the project should be:
1. Setup an AWS EMR that has one master and two nodes.
2. Pyspark read JSON data in S3, the data are SONG_DATA='s3://udacity-dend/song_data' and LOG_DATA='s3://udacity-dend/log_data'.
3. Use Sparksql to create a Fact Table
    songplays - records in event data associated with song plays i.e. records with page NextSong
    (songplay_id, 
    start_time, 
    user_id, 
    level, 
    song_id, 
    artist_id, 
    session_id, 
    location, 
    user_agent)

4. Use Sparksql to create Dimension Tables
   users - users in the app
    (user_id, first_name, last_name, gender, level)
   songs - songs in music database
    (song_id, title, artist_id, year, durationy)
   artists - artists in music database
    (artist_id, name, location, lattitude, longitude)
    time - timestamps of records in songplays broken down into specific units
    (start_time, hour, day, week, month, year, weekday)
    
5. Use UDF.
