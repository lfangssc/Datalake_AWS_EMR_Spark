import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark import SparkContext
from pyspark.sql.types import *

sc=spark.sparkContext

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*"
    
    # read song data file
    df = spark.read.json(song_data).drop_duplicates()

    # extract columns to create songs table
    songs_table = df.select(df.song_id,df.title,df.artist_id,df.year,df.duration).drop_duplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data + "lake/songs.parquet", mode="overwrite", partitionBy=["year","artist_id"])

    # extract columns to create artists table  
    artists_table=df.select(df.artist_id,df.artist_name,df.artist_location,\
                            df.artist_latitude,df.artist_longitude).drop_duplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "lake/artists.parquet", mode="overwrite")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data =input_data + "log_data/*/*/*"

    # read log data file
    df = spark.read.json(log_data).drop_duplicates()
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table    
    users_table = df.select(df.userId,df.firstName,df.lastName,df.gender,df.level).drop_duplicates()
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "lake/users.parquet" , mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.utcfromtimestamp(int(x)/1000), returnType=TimestampType())
    df = df.withColumn('start_time', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    
    # extract columns to create time table
    time_table = df.withColumn("hour",hour("start_time"))\
                    .withColumn("day",dayofmonth("start_time"))\
                    .withColumn("week",weekofyear("start_time"))\
                    .withColumn("month",month("start_time"))\
                    .withColumn("year",year("start_time"))\
                    .withColumn("weekday",dayofweek("start_time"))\
                    .select("start_time","hour", "day", "week", "month", "year", "weekday").drop_duplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data, "lake/time_table.parquet"), mode="overwrite")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "lake/songs.parquet")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(songs_table, df.song == songs_df.title, how='inner')\
                        .select(monotonically_increasing_id().alias("songplay_id"),\
                                col("start_time"),col("userId").alias("user_id"),"level","song_id","artist_id", \
                                col("sessionId").alias("session_id"), col("location"), col("userAgent").alias("user_agent"))

    # write songplays table to parquet files partitioned by year and month
    songplays_table = songplays_table.join(time_table, songplays_table.start_time == time_table.start_time, how="inner")\
                        .select("songplay_id", songplays_table.start_time, "user_id", "level", "song_id",\
                                "artist_id", "session_id", "location", "user_agent", "year", "month")
    
    songplays_table.write.parquet(output_data + "lake/songplays.parquet", mode="overwrite", partitionBy=["year","month"])


def main():
    spark = create_spark_session()
    input_data = "s3://udacity-dend/"
    output_data = "s3://udacity-lake/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
