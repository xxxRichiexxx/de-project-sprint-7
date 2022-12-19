import datetime as dt
import sys

from pyspark.sql import SparkSession
import pyspark.sql.functions as F


def get_events(spark, date, depth, hdfs_url, input_dir, output_dir):

    day = dt.datetime.strptime(date, '%Y-%m-%d')
    input_event_paths =  [f"{hdfs_url}{input_dir}/date={(day-dt.timedelta(days=x)).strftime('%Y-%m-%d')}/" for x in range(int(depth))]

    spark.read.parquet(*input_event_paths) \
                .withColumnRenamed('lat', 'lat2') \
                .withColumnRenamed('lon', 'lng2') \
                .withColumn('user_id',
                    F.when(F.col('event_type') == 'reaction',
                        F.col('event.reaction_from')) \
                    .when(F.col('event_type') == 'subscription',
                        F.col('event.user')) \
                    .otherwise(F.col('event.message_from'))                                  
                ) \
                .withColumn('ts',
                    F.when((F.col('event_type')== 'reaction')|(F.col('event_type') == 'subscription'),
                        F.col('event.datetime')) \
                    .when((F.col('event_type')== 'message')&(F.col('event.message_channel_to').isNotNull()),
                        F.col('event.datetime')) \
                    .otherwise(F.col('event.message_ts')) 
                ) \
                .write \
                .mode('overwrite') \
                .partitionBy('ts', 'user_id') \
                .parquet(hdfs_url + output_dir)


def main():

    hdfs_url = 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020'
  
    spark = SparkSession.builder \
                .appName('geoProject_Andreydzr') \
                .master('local') \
                .getOrCreate()

    # date = '2022-05-21'
    date = sys.argv[1]
    # depth = 2
    depth = sys.argv[2]
    # events_input_path = '/user/master/data/geo/events'
    events_input_path = sys.argv[3]
    # events_output_path = '/user/andreydzr/data/geo_events'
    events_output_path = sys.argv[4]

    get_events(spark, date, depth, hdfs_url, events_input_path, events_output_path)


if __name__ == '__main__':
    main()