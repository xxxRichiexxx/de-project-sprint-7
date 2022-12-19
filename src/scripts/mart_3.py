import datetime as dt
import sys

import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext


def get_events(spark, date, depth, hdfs_url, input_dir):
    """
    Получение датафрейма событий с их координатами.
    """
    day = dt.datetime.strptime(date, '%Y-%m-%d')
    input_event_paths =  [f"{hdfs_url}{input_dir}/date={(day-dt.timedelta(days=x)).strftime('%Y-%m-%d')}/" for x in range(int(depth))]
    return spark \
            .read \
            .parquet(*input_event_paths) \
            .where(
                (F.col('event_type')=='subscription')| \
                (F.col('event_type')=='message')    
            ) \
            .sample(0.001) \
            .cache()


def get_mart_3(events):
    """
    Получение витрины 3.
    """    
    users_in_chanel = events.where(F.col('event_type')=='subscription')\
                            .select('event.subscription_channel', 'event.user')

    user_left = users_in_chanel.withColumnRenamed('user', 'user_left')

    user_right = users_in_chanel.withColumnRenamed('user', 'user_right')

    users_pair = user_left\
                    .join(
                        user_right,
                        [user_left.subscription_channel == user_right.subscription_channel,
                        user_left.user_left != user_right.user_right],
                        'inner'
                    )\
                    .select('user_left', 'user_right')\
                    .distinct()
    
    messages = events \
                .where(
                    (F.col('event_type')=='message')|
                    (F.col('event.message_to').isNotNull())
                ) \
                .cache()                    
    
    contacts =  messages\
                .select('event.message_from', 'event.message_to')\
                .distinct()

    users_pair = users_pair\
                    .join(
                        contacts,
                        [((users_pair.user_left == contacts.message_from)\
                            &(users_pair.user_right == contacts.message_to))|\
                        ((users_pair.user_right == contacts.message_from)\
                            &(users_pair.user_left == contacts.message_to))],
                        'leftanti'
                    )
                    
    window = Window().partitionBy('message_from').orderBy(F.desc('message_ts'))
    user_coordinates = messages \
                        .select(
                            'event.message_from',
                            'lat',
                            'lon',
                            'event.message_ts'
                        ) \
                        .withColumn(
                            'act_lat',
                            F.first('lat',True).over(window)
                        ) \
                        .withColumn(
                            'act_lon',
                            F.first('lon',True).over(window)
                        ) \
                        .withColumn(
                            'ts',
                            F.first('message_ts',True).over(window)
                        ) \
                        .select(
                            'message_from',
                            'act_lat',
                            'act_lon',
                            'ts'
                        ) \
                        .distinct()
                        
    return users_pair \
            .join(user_coordinates, users_pair.user_left == user_coordinates.message_from, 'left') \
            .withColumnRenamed('message_from', 'lu') \
            .withColumnRenamed('act_lat', 'lat1') \
            .withColumnRenamed('act_lon', 'lng1') \
            .join(user_coordinates, users_pair.user_right == user_coordinates.message_from, 'left') \
            .withColumnRenamed('message_from', 'ru') \
            .withColumnRenamed('act_lat', 'lat2') \
            .withColumnRenamed('act_lon', 'lng2') \
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
            .select(
                'user_left',
                'user_right',
                'distance',
                F.current_timestamp().alias('processed_dttm'),
            )
        #     .where(F.col('distance') < 10) \
        #   F.from_utc_timestamp('max(ts)', 'Australia/Sydney').alias('local_time')


def main():

    hdfs_url = 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020'
  
    conf = SparkConf().setAppName("mart3_Andreydzr")
    sc = SparkContext(conf=conf)
    spark = SQLContext(sc)

    date = sys.argv[1]
    depth = sys.argv[2]
    events_input_path = sys.argv[3]
    events = get_events(spark, date, depth, hdfs_url, events_input_path)

    mart_3_dir = sys.argv[4]
    get_mart_3(events) \
        .write \
        .mode('overwrite') \
        .parquet(hdfs_url + mart_3_dir) \


if __name__ == '__main__':
    main()