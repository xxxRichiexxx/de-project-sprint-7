import datetime as dt
import sys
import pandas as pd

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from pyspark.sql.window import Window


def get_cities(url, sep, spark):

    cities = pd.read_csv(url, sep=sep)
    cities['lat'] = cities['lat'].apply(lambda y:y.replace(',','.')).astype(float)
    cities['lng'] = cities['lng'].apply(lambda y:y.replace(',','.')).astype(float)

    return spark \
            .createDataFrame(cities) \
            .withColumnRenamed('lat', 'lat1') \
            .withColumnRenamed('lng', 'lng1') \



def get_events(spark, date, depth, hdfs_url, input_dir):
    """
    Получение датафрейма событий с их координатами.
    """
    day = dt.datetime.strptime(date, '%Y-%m-%d')
    input_event_paths =  [f"{hdfs_url}{input_dir}/date={(day-dt.timedelta(days=x)).strftime('%Y-%m-%d')}/" for x in range(int(depth))]
    return spark.read.parquet(*input_event_paths) \
                .sample(0.000003) \
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
                        F.col('event.datetime'))\
                    .when((F.col('event_type')== 'message')&(F.col('event.message_channel_to').isNotNull()),
                        F.col('event.datetime')) \
                    .otherwise(F.col('event.message_ts')) 
                ) \
                .withColumn('date', F.to_date('ts')) 


def get_events_and_cities(events, cities):
    """
    Получение датафрейма событий с указанием города,
    в котором произошло сбоытие.
    """
    window = Window().partitionBy('event').orderBy('distance')
    return events.join(cities, F.col('event_type')== 'message')\
                    .withColumn(
                        'distance',
                        F.lit(2)*F.lit(6371)*F.acos(
                                F.sqrt(
                                    F.pow(
                                        F.sin(
                                            (F.col('lat2') - F.col('lat1'))/F.lit(2)
                                        ), 2)\
                                    + F.cos('lat1')\
                                    * F.cos('lat2')\
                                    * F.pow(
                                        F.sin(
                                            (F.col('lng2')-F.col('lng1'))/F.lit(2)
                                        ) ,2)
                                )
                        )
                    )\
                    .withColumn('rank', F.rank().over(window))\
                    .where(F.col('rank') == 1)\

def main():

    hdfs_url = 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020'
  
    conf = SparkConf().setAppName("events_&_cities_Andreydzr")
    sc = SparkContext(conf=conf)
    spark = SQLContext(sc)

    # cities_url = 'https://code.s3.yandex.net/data-analyst/data_engeneer/geo.csv'
    cities_url = sys.argv[1]
    sep = ';'
    cities = get_cities(cities_url, sep, spark)

    # date = '2022-06-21'
    date = sys.argv[2]
    # depth = 30
    depth = sys.argv[3]
    # events_input_path = '/user/master/data/geo/events'
    events_input_path = sys.argv[4]
    events = get_events(spark, date, depth, hdfs_url, events_input_path)
    
    # events_output_path = '/user/andreydzr/data/geo_events'
    events_output_path = sys.argv[5]
    get_events_and_cities(events, cities) \
        .write \
        .partitionBy('date','event_type')\
        .mode('overwrite') \
        .parquet(hdfs_url + events_output_path)


if __name__ == '__main__':
    main()