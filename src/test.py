from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import lit
from pyspark.sql.window import Window 

import pandas as pd


base_url = 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020'
events_path = '/user/master/data/geo/events'      
        
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


events = spark.read.parquet(base_url + events_path) \
                        .sample(fraction=0.001) \
                        .withColumnRenamed('lat', 'lat2') \
                        .withColumnRenamed('lon', 'lng2') \
                        
events.write.mode("overwrite").parquet(base_url+'/user/andreydzr/data/tmp/events_geo_sample')

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

user_home = events_and_cities\
                .where(F.col('event_type') == 'message')\
                .select(
                    'event.message_from',
                    'event_type',
                    'date',
                    'city',
                )

user_home.show()