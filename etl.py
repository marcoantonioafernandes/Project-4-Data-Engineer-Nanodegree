import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import TimestampType, DateType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = '{}/song-data/song_data/*/*/*/*.json'.format(input_data)
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(['song_id', 'title', 'artist_id', 'year', 'duration'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table = songs_table.write.partitionBy("year", "artist_id").parquet('{}/songs.parquet'.format(output_data))

    # extract columns to create artists table
    artists_table = df.select(['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude'])
    
    # write artists table to parquet files
    artists_table = artists_table.write.parquet('{}/artists.parquet'.format(output_data))


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = '{}/log-data'.format(input_data)

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select(['userId', 'firstName', 'lastName', 'gender', 'level'])
    
    # write users table to parquet files
    users_table = users_table.write.parquet('{}/users.parquet'.format(output_data))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000), TimestampType())
    df = df.withColumn('timestamp', get_timestamp(col('ts')))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x / 1000), DateType())
    df = df.withColumn('start_time', get_datetime(col('ts')))
    
    # extract columns to create time table
    df = df.withColumn('hour', hour(df.start_time)).withColumn('day', dayofmonth(df.start_time)).withColumn('week', weekofyear(df.start_time)).withColumn('month', month(df.start_time)).withColumn('year', year(df.start_time)).withColumn('weekday', dayofweek(df.start_time))
#     time_table = df.select(['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday'])
    
#     # write time table to parquet files partitioned by year and month
#     time_table = time_table.write.partitionBy("year", 'month').parquet('{}/time.parquet'.format(output_data))

    # read in song data to use for songplays table
    song_df = spark.read.parquet('{}/songs.parquet'.format(output_data))

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, df.song == song_df.title, 'inner').select(['ts', 'userId', 'level', 'song_id', 'artist_id', 'sessionId', 'location', 'userAgent'])

    # write songplays table to parquet files partitioned by year and month
    songplays_table = songplays_table.write.parquet('{}/songplays_table.parquet'.format(output_data))


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-marco/"
    output_data = "s3a://udacity-marco/tables"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    print('Done')


if __name__ == "__main__":
    main()
