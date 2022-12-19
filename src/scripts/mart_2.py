import datetime as dt
import pandas as pd
import sys

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window


def get_cities(url, sep, spark):
    """
    Получение датафрейма городов Австралии с их координатами.
    """ 
    cities = pd.read_csv(url, sep=sep)
    cities['lat'] = cities['lat'].apply(lambda y:y.replace(',','.')).astype(float)
    cities['lng'] = cities['lng'].apply(lambda y:y.replace(',','.')).astype(float)
    return spark \
        .createDataFrame(cities) \
        .withColumnRenamed('lat', 'lat1') \
        .withColumnRenamed('lng', 'lng1')


def get_events(spark, date, depth, hdfs_url, input_dir):
    """
    Получение датафрейма событий с их координатами.
    """
    day = dt.datetime.strptime(date, '%Y-%m-%d')
    input_event_paths =  [f"{hdfs_url}{input_dir}/date={(day-dt.timedelta(days=x)).strftime('%Y-%m-%d')}/" for x in range(int(depth))]
    return spark.read.parquet(*input_event_paths) \
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
                ) 


def get_events_and_cities(events, cities):
    """
    Получение датафрейма событий с указанием города,
    в котором произошло сбоытие.
    """
    window = Window().partitionBy('event').orderBy('distance')
    return events.join(cities)\
                    .withColumn(
                        'distance',
                        F.lit(2)*F.lit(6371)*F.asin(
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
                    .cache()


def get_mart_2(events_and_cities):
    """
    Получение витрины 2.
    """
    w = Window
    return events_and_cities\
            .withColumn('week', F.date_trunc('week', F.col('ts')))\
            .withColumn('month', F.date_trunc('month', F.col('ts')))\
            .withColumn('action_num', F.rank().over(w().partitionBy('user_id', 'event_type').orderBy('ts')))\
            .select(
                'week',
                'month',
                F.col('id').alias('zone_id'),
                F.count(F.when(F.col('event_type') == 'message', 1))\
                    .over(w().partitionBy('week', 'id')).alias('week_message'),
                F.count(F.when(F.col('event_type') == 'reaction', 1))\
                    .over(w().partitionBy('week', 'id')).alias('week_reaction'),
                F.count(F.when(F.col('event_type') == 'subscription', 1))\
                    .over(w().partitionBy('week', 'id')).alias('week_subscription'),
                F.count(F.when((F.col('event_type') == 'message')&(F.col('action_num')==1),1))\
                    .over(w().partitionBy('week', 'id')).alias('week_user'),
                F.count(F.when(F.col('event_type') == 'message', 1))\
                    .over(w().partitionBy('month', 'id')).alias('month_message'),
                F.count(F.when(F.col('event_type') == 'reaction', 1))\
                    .over(w().partitionBy('month', 'id')).alias('month_reaction'),
                F.count(F.when(F.col('event_type') == 'subscription', 1))\
                    .over(w().partitionBy('month', 'id')).alias('month_subscription'),
                F.count(F.when((F.col('event_type') == 'message')&(F.col('action_num')==1),1))\
                    .over(w().partitionBy('month', 'id')).alias('month_user')
            )\
            .distinct()                    


def main():

    hdfs_url = 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020'
  
    spark = SparkSession.builder \
                .config("spark.executor.memory", "8g") \
                .config("spark.executor.cores", 2) \
                .config("spark.driver.memory", "8g") \
                .config("spark.driver.cores", 2) \
                .appName('geoProject') \
                .master('local') \
                .getOrCreate()

    cities_url = sys.argv[1]
    sep = ';'
    cities = get_cities(cities_url, sep, spark)


    date = sys.argv[2]
    depth = sys.argv[3]
    events_input_path = sys.argv[4]
    events = get_events(spark, date, depth, hdfs_url, events_input_path)

    events_and_cities = get_events_and_cities(events, cities)

    mart_2_dir = sys.argv[5]

    get_mart_2(events_and_cities) \
        .write \
        .mode('overwrite') \
        .parquet(hdfs_url + mart_2_dir) \


if __name__ == '__main__':
    main()    