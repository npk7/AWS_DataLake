import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import types as T
from pyspark.sql import functions as F
from datetime import date


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    this function creates spark session and returns it
    
    INPUTS
        none
    OUTPUT
        spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    this function processes song data from the input file.
    
    INPUTS
        spark - spark context
        input_data - path to input data
        output_data - path to output parquet files
    
    the function reads data from S3, processes in HDFS, and attempt to write back to S3
    
    """
    # get filepath to song data file
    song_data = song_data = input_data + "song_data/*/*/*/*.json";
    
    # read song data file
    df = spark.read.json(song_data)
    
    # Deduplication
    df = df.drop_duplicates()

    # extract columns to create songs table
    songs_table = df.select("song_id","title","artist_id","year","duration").drop_duplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year','artist_id').parquet(os.path.join(output_data,'songs_table.parquet'),'overwrite')
    # songs_table.repartition(1).write.mode('overwrite').parquet('songs_table.parquet')

    # extract columns to create artists table
    artists_table = df.select("artist_id", "artist_name","artist_location", "artist_lattitude", "artist_longitude").drop_duplicates() 
    
    # write artists table to parquet files
    # artists_table.repartition(1).write.mode('overwrite').parquet('artists_table.parquet')
    artists_table.write.parquet(os.path.join(output_data,'artists_table.parquet'),'overwrite')



def process_log_data(spark, input_data, output_data):
    """
    this function processes log data from the input file.
    
    INPUTS
        spark - spark context
        input_data - path to input data
        output_data - path to output parquet files
    
    the function reads data from S3, processes in HDFS, and attempt to write back to S3
    
    """
    
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # Deduplication
    df = df.drop_duplicates()
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select("userId","firstName","lastName","gender","level").drop_duplicates()
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users.parquet'),'overwrite')
    # users_table.repartition(2).write.mode('overwrite').parquet('artists_table_from_log_table.parquet')

    # create timestamp column from original timestamp column
    # spark.udf.register('ts_conversion',lambda x: datetime.utcfromtimestamp(int(int(x)/1000)),T.TimestampType())
    get_timestamp = F.udf(lambda x: datetime.fromtimestamp( (x/1000.0) ), T.TimestampType())
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    # get_datetime = udf()
    # df = 
    
    # extract columns to create time table
    # start_time, hour, day, week, month, year, weekday 
    time_table = df.withColumn("hour", hour(col("timestamp")) \
                               .withColumn("day", dayofmonth(col("timestamp"))) \
                               .withColumn("week", weekofyear(col("timestamp"))) \
                               .withColumn("month",month(col("timestamp")) ) \
                               .withColumn("year", year(col("timestamp"))) \
                               .withColumn("weekday",  dayofweek(col("timestamp"))) 
    
    time_table = df.select("ts","timestamp","hour","week","month","year","weekday").drop_duplicates()
                               
    
    # write time table to parquet files partitioned by year and month
    # time_table.repartition(2).write.mode('overwrite').parquet('time_table_from_log_table.parquet')
    time_table.write.partitionBy('year','artist_id').parquet(os.path.join(output_data,'time_table.parquet'),'overwrite')


    # read in song data to use for songplays table
    song_df = spark.read.parquet('data/songs_table.parquet')

    # extract columns from joined song and log datasets to create songplays table 
    df = df.join(song_df, (song_df.title == df.song) & song_df.artist_name == df.artist)
    df = df.withColumn('songplay_uid', monotonically_increasing_id())
    songplays_table = df['songplay_id', 'start_time', 'user_id', 'level', 'song_id', 'artist_id', 'session_id', 'location', 'user_agent']

    # write songplays table to parquet files partitioned by year and month
    # songplays_table.repartition(2).write.mode('overwrite').parquet('songplays.parquet')
    songplays_table.write.partitionBy('year','artist_id').parquet(os.path.join(output_data,'songplays.parquet'),'overwrite')


def main():
    spark = create_spark_session()
    input_data = "s3a://ucity-dend/"
    output_data = "data"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
