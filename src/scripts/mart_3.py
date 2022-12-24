import datetime as dt
import sys
import pandas as pd

import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext


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
                .where(
                    ((F.col('event_type') == 'message') & (F.col('event.message_to').isNotNull())) | (F.col('event_type') == 'subscription')
                ) \
                .sample(0.1) \
                .withColumn('lat2', F.col('lat')/F.lit(57.3)) \
                .withColumn('lng2', F.col('lon')/F.lit(57.3)) \
                .withColumn('user_id',
                    F.when(F.col('event_type') == 'subscription',
                        F.col('event.user')) \
                    .otherwise(F.col('event.message_from'))
                ) \
                .withColumn('ts',
                    F.when(F.col('event_type') == 'subscription',
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

    return messages \
                .join(cities)\
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
                .cache()


def get_mart_3(subscriptions, messages_and_cities):
    """
    Получение витрины 3.
    """
    # Определение пользователей, подписанных на один канал.
    users_in_chanel = subscriptions \
                        .select('event.subscription_channel', 'user_id')

    user_left = users_in_chanel.withColumnRenamed('user_id', 'user_left')

    user_right = users_in_chanel.withColumnRenamed('user_id', 'user_right')

    users_pair = user_left\
                    .join(
                        user_right,
                        [user_left.subscription_channel == user_right.subscription_channel,
                        user_left.user_left != user_right.user_right],
                        'inner'
                    )\
                    .select('user_left', 'user_right')\
                    .distinct()

    # Определение пользователей не переписывающихся друг с другом.
    contacts = messages_and_cities\
                .select('event.message_from', 'event.message_to')\
                .distinct()

    users_pair = users_pair\
                    .join(
                        contacts,
                        [((users_pair.user_left == contacts.message_from)\
                            & (users_pair.user_right == contacts.message_to)) |\
                        ((users_pair.user_right == contacts.message_from)\
                            & (users_pair.user_left == contacts.message_to))],
                        'leftanti'
                    )

    # Определение пользователей, находящихся менее 1 км друг от друга.
    window = Window().partitionBy('user_id').orderBy(F.desc('ts'))
    user_coordinates = messages_and_cities \
                        .select(
                            'user_id',
                            F.first('lat2', True).over(window).alias('act_lat'),
                            F.first('lng2', True).over(window).alias('act_lng'),
                            F.first('id', True).over(window).alias('zone_id'),
                            F.first('ts', True).over(window).alias('act_ts'),
                        ) \
                        .distinct()

    return users_pair \
            .join(user_coordinates, users_pair.user_left == user_coordinates.user_id, 'left') \
            .withColumnRenamed('user_id', 'lu') \
            .withColumnRenamed('act_lat', 'lat1') \
            .withColumnRenamed('act_lng', 'lng1') \
            .withColumnRenamed('zone_id', 'zone_id1') \
            .withColumnRenamed('act_ts', 'act_ts1') \
            .join(user_coordinates, users_pair.user_right == user_coordinates.user_id, 'left') \
            .withColumnRenamed('user_id', 'ru') \
            .withColumnRenamed('act_lat', 'lat2') \
            .withColumnRenamed('act_lng', 'lng2') \
            .withColumnRenamed('zone_id', 'zone_id2') \
            .withColumnRenamed('act_ts', 'act_ts2') \
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
            ) \
            .where(F.col('distance') <= 30) \
            .select(
                'user_left',
                'user_right',
                F.current_timestamp().alias('processed_dttm'),
                F.when(F.col('zone_id1') == F.col('zone_id2'), F.col('zone_id1')).alias('zone_id'),
                F.from_utc_timestamp(F.current_timestamp(), 'Australia/Sydney').alias('local_time'),
            )


def main():

    hdfs_url = 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020'

    conf = SparkConf().setAppName("mart3_Andreydzr")
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
    subscriptions = events.where(F.col('event_type') == 'subscription')

    messages_and_cities = get_messages_and_cities(messages, cities)

    mart_3_dir = sys.argv[5]

    get_mart_3(subscriptions, messages_and_cities) \
        .write \
        .mode('overwrite') \
        .parquet(hdfs_url + mart_3_dir)


if __name__ == '__main__':
    main()
