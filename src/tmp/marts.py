from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import lit
from pyspark.sql.window import Window
from pyspark import StorageLevel


def get_events_and_cities(events, cities):

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
                    .persist(StorageLevel.MEMORY_AND_DISK_ONLY)

withColumn('dif', F.acos(F.sin(F.col('lat_double_fin'))F.sin(F.col('lat_to')) + F.cos(F.col('lat_double_fin'))F.cos(F.col('lat_to'))F.cos(F.col('lng_double_fin')-F.col('lng_to')))F.lit(6371) )\
.filter(F.col('dif')<=1)

def get_act_city(events_and_cities):
    
    window = Window().partitionBy('user_id').orderBy(F.desc('ts'))

    return events_and_cities\
                .select(
                    'user_id',
                    F.first('city',True).over(window).alias('act_city')
                )\
                .distinct()


def get_visits(events_and_cities):

    window = Window().partitionBy('user_id').orderBy('date')

    return events_and_cities\
                .select(
                    'user_id',
                    F.to_date('ts').alias('date'),
                    'city'                   
                ) \
                .distinct() \
                .withColumn('prev_city', F.lag('city').over(window))\
                .withColumn(
                    'num_visit', 
                    F.when(
                        (F.col('city')!=F.col('prev_city'))|(F.col('prev_city').isNull()),
                        F.monotonically_increasing_id()
                    )
                ) \
                .withColumn('num_visit_full', F.max('num_visit').over(window))\
                .persist(StorageLevel.MEMORY_AND_DISK_ONLY)


def get_home_city(visits):

    window = Window().partitionBy('user_id').orderBy(F.desc('num_visit_full'))   

    return visits\
            .groupBy('user_id', 'city', 'num_visit_full').count() \
            .where(F.col('count') > 3) \
            .select(
                'user_id',
                F.first('city').over(window).alias('home_city')
            )\
            .persist(StorageLevel.MEMORY_AND_DISK_ONLY)


def get_travel_count(visits):
    return visits\
            .groupBy('user_id')\
            .agg(F.count_distinct('num_visit_full').alias('travel_count'))


def get_travel_array(visits):
    return visits\
            .select(
                'user_id',
                'city',
                'num_visit_full'
            )\
            .distinct()\
            .groupBy('user_id')\
            .agg(F.collect_list('city')).alias('travel_array')


def get_local_time(events_and_cities):

    window = Window().partitionBy('user_id').orderBy(F.desc('ts'))

    return events_and_cities\
                .withColumn('last_city', F.first('city').over(window))\
                .select(
                'user_id',
                F.from_utc_timestamp(F.col("ts"),F.concat(F.lit("Australia/"),F.col('city')))
                )\
                .distinct()

def get_mart_1(act_city, home_city, travel_count, travel_array, local_time):
    return act_city \
                .join(home_city, 'user_id', 'full') \
                .join(travel_count, 'user_id', 'full') \
                .join(travel_array, 'user_id', 'full') \
                .join(local_time, 'user_id', 'full')


def get_zones(events_and_cities):
    
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
                     
# zones.show()

# users_in_chanel = events.where(F.col('event_type')=='subscription')\
#                         .select('event.subscription_channel', 'event.user')

# user_left = users_in_chanel.withColumnRenamed('user', 'user_left')

# user_right = users_in_chanel.withColumnRenamed('user', 'user_right')

# users_pair = user_left\
#                     .join(
#                         user_right,
#                         [user_left.subscription_channel == user_right.subscription_channel,
#                          user_left.user_left != user_right.user_right],
#                         'inner'
#                     )\
#                     .select('user_left', 'user_right')\
#                     .distinct()\
#                     .cache()
                    

# contacts = events\
#             .where(F.col('event_type')=='message')\
#             .select('event.message_from', 'event.message_to')\
#             .distinct()\
#             .cache()

# users_without_contacts = users_pair\
#                             .join(
#                                 contacts,
#                                 [((users_pair.user_left == contacts.message_from)&(users_pair.user_right == contacts.message_to))|\
#                                  ((users_pair.user_right == contacts.message_from)&(users_pair.user_left == contacts.message_to))],
#                                 'leftanti'
#                             )\
#                             .orderBy('user_left')\

def main():

    hdfs_url = 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020'
  
    spark = SparkSession.builder \
                .appName('geoProject') \
                .master('local') \
                .getOrCreate()

    cities_path = 'user/andreydzr/data/cities'
    cities = spark.read.parquet(hdfs_url + cities_path)

    events_path = '/user/andreydzr/data/geo_events'
    events = spark.read.parquet(hdfs_url + events_path)

    events_and_cities = get_events_and_cities(events, cities)

    act_city = get_act_city(events_and_cities)

    visits = get_visits(events_and_cities)

    home_city = get_home_city(visits)

    travel_count = get_travel_count(visits)

    travel_array = get_travel_array(visits)

    local_time = get_local_time(events_and_cities)

    mart_1_dir = 'user/andreydzr/data/analytics/mart_1'

    get_mart_1(act_city, home_city, travel_count, travel_array, local_time) \
        .write \
        .mode('overwrite') \
        .parquet(hdfs_url + mart_1_dir)

    mart_2_dir = 'user/andreydzr/data/analytics/mart_2'

    get_zones(events_and_cities) \
        .write \
        .mode('overwrite') \
        .parquet(hdfs_url + mart_2_dir)     


if __name__ == '__main__':
    main()