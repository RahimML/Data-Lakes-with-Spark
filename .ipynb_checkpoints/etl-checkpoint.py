import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Dat, TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']= config.get('AWSUSER', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']= config.get('AWSUSER', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    

    # fix song schema 
    songSchema = R([
        Fld("artist_id",Str()),
        Fld("artist_latitude",Dbl()),
        Fld("artist_location",Str()),
        Fld("artist_longitude",Dbl()),
        Fld("artist_name",Str()),
        Fld("duration",Dbl()),
        Fld("num_songs",Int()),
        Fld("title",Str()),
        Fld("year",Int()),
    ])
    
    # read song data file
    df = spark.read.json( song_data , schema = songSchema)
    
    song_fields = [ "title" , "artist_id" , "year" , "duration" ]
    
    
    # write songs table to parquet files partitioned by year and artist
    
    songs_table = (df
               .select(song_fields)
               .dropDuplicates().distinct()
               .withColumn("song_id", monotonically_increasing_id())
              )
    
    # write songs table to parquet files partitioned by year and artist
    
    (songs_table
     .write
     .partitionBy("year", "artist_id")
     .parquet(output_data + 'songs/')
    )
    
    # extract columns to create artists table
    
    artists_fields = ["artist_id",
                  "artist_name as name",
                  "artist_location as location",
                  "artist_latitude as latitude",
                  "artist_longitude as longitude"]
    
    artists_table = df.selectExpr(artists_fields).dropDuplicates().distinct()
    
   # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists/')



def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextPage')

    # extract columns for users table
    user_fields = ["userId as user_id",
                  "firstName as first_name",
                  "lastName as last_name",
                  "gender",
                  "level"]
    users_table = (df.selectExpr(user_fields).dropDuplicates().distinct()
              )
    
    # write users table to parquet files
    users_table.write.parquet(output_data + 'users/')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda u: datetime.fromtimestamp(u/1000) , TimestampType())
    df = df.withColumn("timestamp", get_timestamp(col("ts")))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda u: to_date(u) , TimestampType())
    df = df.withColumn("start_time" , get_datetime(col("ts")))
    
    # extract columns to create time table
    df = df.withColumn("hour" , hour("timestamp"))
    df = df.withColumn("day" , dayofmonth("timestamp"))
    df = df.withColumn("week" , week("timestamp"))
    df = df.withColumn("month" , month("timestamp"))
    df = df.withColumn("year" , year("timestamp"))
    df = df.withColumn("weekday" , dayofweek("timestamp"))
    time_table = df.select(col("timestamp"),col("hour"),col("day") , col("week"), col("month"), col("year"), col("weekday")).dropDuplicates().distinct()

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(output_data + 'time/')

    # read in song data to use for songplays table
    song_df =  spark.read.parquet(output_data + 'songs/*/*/*')
    artist_df =  spark.read.parquet(output_data + 'artists/*')

    # extract columns from joined song and log datasets to create songplays table 
    songs_logs_join = df.join(song_df, (df.songs == song_df.title))
    
    songs_logs_artist_join = songs_logs_join.join(artist_df, songs_logs_join.artitst == artist_df.name )
    
    songplays= songs_logs_artist_join.join( time_table,
    songs_logs_artist_join.ts == time_table.start_time,'left').drop(songs_logs_artist_join.year)
    
    songplays_table = songplays.select( col('start_time').alias('start_time'),
        col('userId').alias('user_id') ,
        col('level').alias('level') ,
        col('song_id').alias('song_id') ,
        col('artist_id').alias('artist_id') ,
        col('sessionId').alias('session_id') ,
        col('location').alias('location') ,
        col('userAgent').alias('user_agent') ,
        col('year').alias('year') ,
        col('month').alias('month') ,
    ).repartition("year", "month")
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionby("year", "month").parquet(output_data + 'songplays/')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://spark-project4/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
