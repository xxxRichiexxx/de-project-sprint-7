from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import lit
from pyspark.sql.window import Window 

import pandas as pd
import datetime as dt


base_url = 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020'
events_path = '/user/master/data/geo/events'

def input_event_paths(date, depth):
    day = dt.datetime.strptime(date, '%Y-%m-%d')
    return [f"{base_url}{events_path}/date={(day-dt.timedelta(days=x)).strftime('%Y-%m-%d')}/" for x in range(int(depth))]
        
spark = SparkSession.builder \
            .appName('geoProject') \
            .master('local') \
            .getOrCreate()

cities = pd.read_csv('https://code.s3.yandex.net/data-analyst/data_engeneer/geo.csv', sep=';')

cities['lat'] = cities['lat'].apply(lambda y:y.replace(',','.')).astype(float)
cities['lng'] = cities['lng'].apply(lambda y:y.replace(',','.')).astype(float)

cities = spark.createDataFrame(cities) \
                .withColumnRenamed('lat', 'lat1') \
                .withColumnRenamed('lng', 'lng1') \
                .cache()


events = spark.read.parquet(*input_event_paths('2022-05-21',20)) \
                        .withColumnRenamed('lat', 'lat2') \
                        .withColumnRenamed('lon', 'lng2') \
                        .cache()

# events.printSchema()                        

events_and_cities = events.join(cities)

window = Window().partitionBy('event').orderBy('distance')

events_and_cities = events_and_cities\
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
                        .where(F.col('rank') == 1) \
                        .cache()
# events_and_cities.show()

window = Window().partitionBy('event.message_from').orderBy(F.desc('event.message_ts'))

act_city = events_and_cities\
                .where(F.col('event_type') == 'message')\
                .select(
                    'event.message_from',
                    F.first('city',True).over(window).alias('act_city')
                )\
                .distinct()

# act_city.show()

window = Window().partitionBy('message_from').orderBy('date')
window_2 = Window().partitionBy('message_from').orderBy(F.desc('num_visit_full'))   

home_city = events_and_cities\
                .where(F.col('event_type') == 'message')\
                .select(
                    'event.message_from',
                    F.to_date('event.message_ts').alias('date'),
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
                .withColumn('num_visit_full', F.max('num_visit').over(window)) \
                .groupBy('message_from', 'city', 'num_visit_full').count() \
                .where(F.col('count') > 6) \
                .select(
                    'message_from',
                    F.first('city').over(window_2).alias('home_city')
                )
                
# home_city.show(50)

result = act_city.join(home_city, 'message_from', 'left').orderBy(F.desc('home_city'))
result.show(100)
