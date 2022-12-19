import os
import datetime as dt

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8' 


default_args = {
    'owner': 'Швейников Андрей',
}
with DAG(
        'events',
        default_args=default_args,
        description='Получение данных о событиях.',
        start_date=dt.datetime(2022,11,22),
        # schedule_interval='@daily',
        catchup=True,
        max_active_runs = 1,
) as dag:

    mart_1 = SparkSubmitOperator(
        task_id='mart_1',
        application ='/lessons/scripts/mart_1.py',
        conn_id= 'yarn_spark',
        application_args = [
            'https://code.s3.yandex.net/data-analyst/data_engeneer/geo.csv',
            '2022-05-21',
            3,
            '/user/master/data/geo/events',
            '/user/andreydzr/data/analytics/mart_1',
        ],
    )

    mart_2 = SparkSubmitOperator(
        task_id='mart_2',
        application ='/lessons/scripts/mart_2.py',
        conn_id= 'yarn_spark',
        application_args = [
            'https://code.s3.yandex.net/data-analyst/data_engeneer/geo.csv',
            '2022-05-21',
            3,
            '/user/master/data/geo/events',
            '/user/andreydzr/data/analytics/mart_2',
        ],
    )

    mart_3 = SparkSubmitOperator(
        task_id='mart_3',
        application ='/lessons/scripts/mart_3.py',
        conn_id= 'yarn_spark',
        application_args = [
            '2022-05-21',
            3,
            '/user/master/data/geo/events',
            '/user/andreydzr/data/analytics/mart_2',
        ],
    )

    [mart_1, mart_2] >> mart_3

