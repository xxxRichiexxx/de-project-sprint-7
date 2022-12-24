import datetime as dt
import pandas as pd
import sys

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from pyspark.sql.window import Window


def get_cities(url, sep, spark):
    """
    Получение датафрейма городов Австралии с их координатами.
    """
    cities = pd.read_csv(url, sep=sep)
    cities['lat'] = cities['lat'].apply(
        lambda y: y.replace(',', '.')
    ).astype('float64')
    cities['lng'] = cities['lng'].apply(
        lambda y: y.replace(',', '.')
    ).astype('float64')
    return spark \
        .createDataFrame(cities) \
        .withColumn('lat1', F.col('lat')/F.lit(57.3)) \
        .withColumn('lng1', F.col('lng')/F.lit(57.3))


def get_events(spark, date, depth, hdfs_url, input_dir):
    """
    Получение датафрейма событий с их координатами.
    """
    day = dt.datetime.strptime(date, '%Y-%m-%d')
    input_event_paths = [f"{hdfs_url}{input_dir}/date={(day-dt.timedelta(days=x)).strftime('%Y-%m-%d')}/" for x in range(int(depth))]
    return spark.read.parquet(*input_event_paths) \
                .sample(0.03) \
                .withColumn('lat2', F.col('lat')/F.lit(57.3)) \
                .withColumn('lng2', F.col('lon')/F.lit(57.3)) \
                .withColumn('user_id',
                    F.when(F.col('event_type') == 'reaction',
                        F.col('event.reaction_from')) \
                    .when(F.col('event_type') == 'subscription',
                        F.col('event.user')) \
                    .otherwise(F.col('event.message_from'))
                ) \
                .withColumn('ts',
                    F.when((F.col('event_type') == 'reaction') | (F.col('event_type') == 'subscription'),
                        F.col('event.datetime'))\
                    .when((F.col('event_type') == 'message') & (F.col('event.message_channel_to').isNotNull()),
                        F.col('event.datetime')) \
                    .otherwise(F.col('event.message_ts'))
                ) \
                .drop('lat', 'lon', 'date') \
                .cache()


def get_messages_and_cities(messages, cities):
    """
    Получение датафрейма сообщений с указанием города,
    из которого было отправлено сообщение.
    """
    window = Window().partitionBy('event.message_id').orderBy('distance')

    return messages.join(cities) \
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
                                        ), 2)
                                )
                        )
                    )\
                    .withColumn('rank', F.rank().over(window))\
                    .where(F.col('rank') == 1)\
                    .select(
                        'user_id',
                        'event_type',
                        F.col('id').alias('zone_id'),
                        'ts',
                    ) \
                    .cache()


def get_act_city(messages_and_cities):
    """
    Получение города, из которого было отправлено последнее сообщение пользователем.
    """
    window = Window().partitionBy('user_id').orderBy(F.desc('ts'))
    return messages_and_cities \
                .select(
                    'user_id',
                    F.first('zone_id', True).over(window).alias('zone_id'),
                ) \
                .distinct()


def get_other_events_with_cities(other_events, act_city):
    """
    Получение других событий за данный период с указанием города из которого было отправлено последнее сообщение
    пользователем.
    """
    return other_events \
                .join(act_city, 'user_id', 'left') \
                .select(
                    'user_id',
                    'event_type',
                    'zone_id',
                    'ts',
                )


def get_all_events(messages_and_cities, events_with_cities):
    """
    Получение датафрейма всех событий за данный период.
    """
    return messages_and_cities.union(events_with_cities)


def get_mart_2(all_events):
    """
    Получение витрины 2.
    """
    w = Window
    return all_events\
            .withColumn('week', F.date_trunc('week', F.col('ts'))) \
            .withColumn('month', F.date_trunc('month', F.col('ts'))) \
            .withColumn('action_num', F.rank().over(w().partitionBy('user_id', 'event_type').orderBy('ts')))\
            .select(
                'month',
                'week',
                'zone_id',
                F.count(F.when(F.col('event_type') == 'message', 1)) \
                    .over(w().partitionBy('week', 'zone_id')).alias('week_message'),
                F.count(F.when(F.col('event_type') == 'reaction', 1)) \
                    .over(w().partitionBy('week', 'zone_id')).alias('week_reaction'),
                F.count(F.when(F.col('event_type') == 'subscription', 1)) \
                    .over(w().partitionBy('week', 'zone_id')).alias('week_subscription'),
                F.count(F.when((F.col('event_type') == 'message') & (F.col('action_num') == 1), 1)) \
                    .over(w().partitionBy('week', 'zone_id')).alias('week_user'),
                F.count(F.when(F.col('event_type') == 'message', 1)) \
                    .over(w().partitionBy('month', 'zone_id')).alias('month_message'),
                F.count(F.when(F.col('event_type') == 'reaction', 1)) \
                    .over(w().partitionBy('month', 'zone_id')).alias('month_reaction'),
                F.count(F.when(F.col('event_type') == 'subscription', 1)) \
                    .over(w().partitionBy('month', 'zone_id')).alias('month_subscription'),
                F.count(F.when((F.col('event_type') == 'message') & (F.col('action_num') == 1), 1)) \
                    .over(w().partitionBy('month', 'zone_id')).alias('month_user')
            ) \
            .distinct()


def main():

    hdfs_url = 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020'

    conf = SparkConf().setAppName("mart_1_Andreydzr")
    sc = SparkContext(conf=conf)
    spark = SQLContext(sc)

    cities_url = sys.argv[1]
    sep = ';'
    cities = get_cities(cities_url, sep, spark)

    date = sys.argv[2]
    depth = sys.argv[3]
    events_input_path = sys.argv[4]
    events = get_events(spark, date, depth, hdfs_url, events_input_path)

    messages = events.where(F.col('event_type') == 'message')

    other_events = events.where(F.col('event_type') != 'message')

    messages_and_cities = get_messages_and_cities(messages, cities)

    act_city = get_act_city(messages_and_cities)

    events_with_cities = get_other_events_with_cities(other_events, act_city)

    all_events = get_all_events(messages_and_cities, events_with_cities)

    mart_2_dir = sys.argv[5]

    get_mart_2(all_events) \
        .write \
        .mode('overwrite') \
        .parquet(hdfs_url + mart_2_dir)


if __name__ == '__main__':
    main()
