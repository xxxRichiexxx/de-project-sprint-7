import pandas as pd
import sys

from pyspark.sql import SparkSession


def get_cities(url, sep, spark, hdfs_url, dir):

    cities = pd.read_csv(url, sep=sep)
    cities['lat'] = cities['lat'].apply(lambda y:y.replace(',','.')).astype(float)
    cities['lng'] = cities['lng'].apply(lambda y:y.replace(',','.')).astype(float)

    spark \
        .createDataFrame(cities) \
        .withColumnRenamed('lat', 'lat1') \
        .withColumnRenamed('lng', 'lng1') \
        .write \
        .mode('overwrite') \
        .parquet(hdfs_url + dir)


def main():

    hdfs_url = 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020'
  
    spark = SparkSession.builder \
                .appName('geoProject_Andreydzr') \
                .master('local') \
                .getOrCreate()

    cities_url = sys.argv[1]
    sep = ';'
    cities_path = sys.argv[2]

    get_cities(cities_url, sep, spark, hdfs_url, cities_path)



if __name__ == '__main__':
    main()