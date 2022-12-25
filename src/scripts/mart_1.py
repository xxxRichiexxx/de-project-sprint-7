import datetime as dt
import pandas as pd
import sys
from decimal import Decimal

import os

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

import findspark

findspark.init()
findspark.find()

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
    ).apply(Decimal)
    cities['lng'] = cities['lng'].apply(
        lambda y: y.replace(',', '.')
    ).apply(Decimal)
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
                .where(F.col('event_type') == 'message') \
                .sample(0.5) \
                .withColumn('lat2', F.col('lat')/F.lit(57.3)) \
                .withColumn('lng2', F.col('lon')/F.lit(57.3)) \
                .withColumn('user_id', F.col('event.message_from')) \
                .withColumn('ts',
                    F.when(F.col('event.message_channel_to').isNotNull(),
                        F.col('event.datetime')) \
                    .otherwise(F.col('event.message_ts'))
                ) \
                .drop('event_type', 'lat', 'lon', 'date')


def get_events_and_cities(events, cities):
    """
    Получение датафрейма событий с указанием города,
    в котором произошло сбоытие.
    """
    window = Window().partitionBy('event.message_id').orderBy('distance')

    return events.join(cities) \
                    .withColumn(
                        'distance',
                        F.lit(2)*F.lit(6371)*F.asin(
                                F.sqrt(
                                    F.pow(
                                        F.sin(
                                            (F.col('lat2') - F.col('lat1'))/F.lit(2)
                                        ), 2) \
                                    + F.cos('lat1') \
                                    * F.cos('lat2') \
                                    * F.pow(
                                        F.sin(
                                            (F.col('lng2')-F.col('lng1'))/F.lit(2)
                                        ), 2)
                                )
                        )
                    ) \
                    .withColumn('rank', F.rank().over(window)) \
                    .where(F.col('rank') == 1) \
                    .cache()


def get_act_city(events_and_cities):
    """
    Получение города, из которого было отправлено последнее событие пользователем
    и местного времени.
    """
    window = Window().partitionBy('user_id').orderBy(F.desc('ts'))
    return events_and_cities \
                .select(
                    'user_id',
                    F.first('city', True).over(window).alias('act_city'),
                    F.from_utc_timestamp(
                        F.first('ts', True).over(window).alias('act_city'),
                        F.concat(F.lit("Australia/"), F.lit('Sydney')),
                    ).alias('local_time'),
                ) \
                .distinct()


def get_visits(events_and_cities):
    """
    На основе датафрейма событий с указанием города,
    получаем новый датафрейм, в котором указаны номера визитов
    по следующему алгоритму:
     - для каждого пользователя рассматриваем события с течением времени;
     - если события происходят подряд в одном городе, то это один визит;
     - если в потоке событий происходит смена города, то это новый визит.
    """
    window = Window().partitionBy('user_id').orderBy('date')
    return events_and_cities \
                .select(
                    'user_id',
                    F.to_date('ts').alias('date'),
                    'city'
                ) \
                .distinct() \
                .withColumn('prev_city', F.lag('city').over(window)) \
                .withColumn(
                    'num_visit',
                    F.when(
                        (F.col('city') != F.col('prev_city')) | (F.col('prev_city').isNull()),
                        F.monotonically_increasing_id()
                    )
                ) \
                .withColumn('num_visit_full', F.max('num_visit').over(window)) \
                .cache()


def get_home_city(visits):
    """
     Получение домашнего адреса пользователя.
     Это последний город, в котором пользователь был дольше 10 дней.
    """
    window = Window().partitionBy('user_id').orderBy(F.desc('num_visit_full'))
    return visits \
            .groupBy('user_id', 'city', 'num_visit_full').count() \
            .where(F.col('count') > 10) \
            .select(
                'user_id',
                F.first('city').over(window).alias('home_city')
            )


def get_travel_count(visits):
    """
    Количество посещённых городов.
    Если пользователь побывал в каком-то городе повторно,
    то это считается за отдельное посещение.
    """
    return visits \
            .groupBy('user_id') \
            .agg(F.count_distinct('num_visit_full').alias('travel_count'))


def get_travel_array(visits):
    """
    Получение списка городов в порядке посещения пользователем.
    """
    return visits \
            .select(
                'user_id',
                'city',
                'num_visit_full'
            ) \
            .distinct() \
            .orderBy('user_id', 'num_visit_full') \
            .groupBy('user_id') \
            .agg(F.collect_list('city')).alias('travel_array')


def get_mart_1(act_city, home_city, travel_count, travel_array):
    """
    Сборка отдельных метрик в единую витрину.
    """
    return act_city \
            .join(home_city, 'user_id', 'full') \
            .join(travel_count, 'user_id', 'full') \
            .join(travel_array, 'user_id', 'full')


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

    events_and_cities = get_events_and_cities(events, cities)

    act_city = get_act_city(events_and_cities)

    visits = get_visits(events_and_cities)

    home_city = get_home_city(visits)

    travel_count = get_travel_count(visits)

    travel_array = get_travel_array(visits)

    mart_1_dir = sys.argv[5]

    get_mart_1(act_city, home_city, travel_count, travel_array) \
        .write \
        .mode('overwrite') \
        .parquet(hdfs_url + mart_1_dir)


if __name__ == '__main__':
    main()
