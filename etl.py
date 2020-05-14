import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql import functions
from pyspark.sql.types import DateType, IntegerType, TimestampType
from datetime import datetime


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Creates a spark session
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """imports song data from S3 bucket, transforms it into databases based on a star-schema approach and saves data in parquet files using pyspark"""
    
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    print("Getting song data...")
    df = spark.read.json(song_data)
    print("Song data loaded.")
    print(df.head())

    # extract columns to create songs table
    songs_table = df['song_id', 'title', 'artist_id', 'year', 'duration']
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy(["year", "artist_id"]).parquet(os.path.join(output_data,"song.parquet"),"overwrite")

    # extract columns to create artists table
    artists_table = df['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data,"artists.parquet"),"overwrite")


def process_log_data(spark, input_data, output_data):
    """imports log data from S3 bucket, transforms it into databases based on a star-schema approach and saves data in parquet files """
    
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    print("Getting log data...")
    df = spark.read.json(log_data)
    print("Log data loaded.")
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table =df.select(
                            col("userId").alias("user_id"),
                            col("firstName").alias("first_name"),
                            col("lastName").alias("last_name"),
                            col("gender"),
                            col("level")
                        )

    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data,"user.parquet"),"overwrite")

    # create timestamp column from original timestamp column
    def format_datetime(ts):
        return datetime.fromtimestamp(ts/1000.0)

    get_timestamp = udf(lambda x: format_datetime(int(x)),TimestampType())
    df = df.withColumn("timestamp", get_timestamp(df.ts))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: format_datetime(int(x)),DateType())
    df = df.withColumn("datetime", get_timestamp(df.ts))

    
    # extract columns to create time table
    time_table = df.select(
                            functions.col('timestamp').alias('start_time'),
                            functions.hour('datetime').alias('hour'),
                            functions.dayofmonth('datetime').alias('day'),
                            functions.weekofyear('datetime').alias('week'),
                            functions.month('datetime').alias('month'),
                            functions.year('datetime').alias('year'),
                            functions.date_format('datetime', 'u').alias('weekday')
                            )
    time_table = time_table.drop_duplicates(subset=['start_time'])
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy(["year", "month"]).parquet(os.path.join(output_data,"time.parquet"),"overwrite")

    # read in song data to use for songplays table
    song_df = spark.read.parquet( os.path.join(output_data,"song.parquet") )
    ntable = df.join(song_df, df.song == song_df.title,how="inner")
    

    # extract columns from joined song and log datasets to create songplays table 
    #songplays_table = ntable["ts","userId","level","song_id","artist_id","sessionId","location","userAgent"]
    
    songplays_table=ntable.select(
                                    functions.month('datetime').alias('month'),
                                    functions.year('datetime').alias('year'),
                                    col("userId").alias("user_id"),
                                    col("level"),
                                    col("song_id"),
                                    col("artist_id"),
                                    col("sessionId").alias("session_id"),
                                    col("location"),
                                    col("userAgent").alias("user_agent")
                                    )
    
    songplays_table=songplays_table.withColumn("songplay_id", monotonically_increasing_id() )
    
    print(songplays_table.head())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy(["year", "month"]).parquet(os.path.join(output_data,"songplays.parquet"),"overwrite")


def main():
    """Main function runs process_song_data and process_log_data"""
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    #process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
