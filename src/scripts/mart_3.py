import datetime as dt
import sys

from pyspark.sql import SparkSession
import pyspark.sql.functions as F


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
            .cache()


def get_mart_3(events, spark, hdfs_url):
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
                        
    contacts = events\
                .where(F.col('event_type')=='message')\
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

    users_with_home_city = spark \
                            .read \
                            .parquet(hdfs_url + '/user/andreydzr/data/analytics/mart_1')                      
    
    return users_pair \
                    .join(
                        users_with_home_city,
                        users_pair.user_left == users_with_home_city.user_id
                    ) \
                    .withColumnRenamed('home_city', 'user_left_home_city') \
                    .withColumnRenamed('user_id', 'lu') \
                    .join(
                        users_with_home_city,
                        users_pair.user_right == users_with_home_city.user_id
                    ) \
                    .withColumnRenamed('home_city', 'user_right_home_city') \
                    .withColumnRenamed('user_id', 'ru') \
                    .where(F.col('user_left_home_city') == F.col('user_right_home_city')) \
                    .select(
                        'user_left',
                        'user_right',
                        F.current_timestamp().alias('processed_dttm'),
                        F.col('user_left_home_city').alias('city'),
                        F.from_utc_timestamp(F.current_timestamp(), 'Australia/Sydney').alias('local_time')
                    )


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

    date = sys.argv[1]
    depth = sys.argv[2]
    events_input_path = sys.argv[3]
    events = get_events(spark, date, depth, hdfs_url, events_input_path)

    mart_3_dir = sys.argv[4]
    get_mart_3(events, spark, hdfs_url) \
        .write \
        .mode('overwrite') \
        .parquet(hdfs_url + mart_3_dir) \


if __name__ == '__main__':
    main()