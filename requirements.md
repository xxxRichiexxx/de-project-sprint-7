# Выполнение проекта:
### 1) Изучаем источник данных

    from pyspark.sql import SparkSession


    hdfs_url = 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020'

    spark = SparkSession.builder \
                    .appName('datareader') \
                    .master('local') \
                    .getOrCreate()

    data = spark.read.parquet(hdfs_url + '/user/master/data/geo/events')
    data.printSchema()
    data.show()

    root
    |-- event: struct (nullable = true)
    |    |-- admins: array (nullable = true)
    |    |    |-- element: long (containsNull = true)
    |    |-- channel_id: long (nullable = true)
    |    |-- datetime: string (nullable = true)
    |    |-- media: struct (nullable = true)
    |    |    |-- media_type: string (nullable = true)
    |    |    |-- src: string (nullable = true)
    |    |-- message: string (nullable = true)
    |    |-- message_channel_to: long (nullable = true)
    |    |-- message_from: long (nullable = true)
    |    |-- message_group: long (nullable = true)
    |    |-- message_id: long (nullable = true)
    |    |-- message_to: long (nullable = true)
    |    |-- message_ts: string (nullable = true)
    |    |-- reaction_from: string (nullable = true)
    |    |-- reaction_type: string (nullable = true)
    |    |-- subscription_channel: long (nullable = true)
    |    |-- subscription_user: string (nullable = true)
    |    |-- tags: array (nullable = true)
    |    |    |-- element: string (containsNull = true)
    |    |-- user: string (nullable = true)
    |-- event_type: string (nullable = true)
    |-- lat: double (nullable = true)
    |-- lon: double (nullable = true)
    |-- date: date (nullable = true)

    +--------------------+----------+-------------------+------------------+----------+
    |               event|event_type|                lat|               lon|      date|
    +--------------------+----------+-------------------+------------------+----------+
    |{null, null, 2022...|  reaction|-26.728842867329007|152.90596937369952|2022-05-30|
    |{null, null, 2022...|  reaction| -31.30616755408133|116.38841832283045|2022-05-30|
    |{null, null, 2022...|  reaction|-22.568566129738795|150.53405510107592|2022-05-30|
    |{null, null, 2022...|  reaction|-12.433534102119754|131.35071038228722|2022-05-30|
    |{null, null, 2022...|  reaction|-12.377345240540057|131.51896908518478|2022-05-30|
    |{null, null, 2022...|  reaction|-26.967101428948645|153.78804052958333|2022-05-30|
    |{null, null, 2022...|  reaction|-11.685460691926384| 130.8833764307162|2022-05-30|
    |{null, null, 2022...|  reaction| -26.74671387818525|152.08417038964188|2022-05-30|
    |{null, null, 2022...|  reaction| -26.94158521928808|152.92817870766552|2022-05-30|
    |{null, null, 2022...|  reaction|-27.096374536978264| 153.7055798510923|2022-05-30|
    |{null, null, 2022...|  reaction|-31.578565211860152| 116.7740675688588|2022-05-30|
    |{null, null, 2022...|  reaction|-20.590832803495896| 149.8285715001092|2022-05-30|
    |{null, null, 2022...|  reaction|-33.017235885303265|151.54329054448309|2022-05-30|
    |{null, null, 2022...|  reaction| -42.58131416020049|  148.316653098671|2022-05-30|
    |{null, null, 2022...|  reaction|-23.326804758843508|151.46875211497198|2022-05-30|
    |{null, null, 2022...|  reaction| -32.02698504412505|  152.343418309737|2022-05-30|
    |{null, null, 2022...|  reaction| -32.34016902616169| 151.8414627355911|2022-05-30|
    |{null, null, 2022...|  reaction|-37.753302429148235|145.20284200238538|2022-05-30|
    |{null, null, 2022...|  reaction|-22.670316610382454|150.87743572596122|2022-05-30|
    |{null, null, 2022...|  reaction| -33.41293277262922|151.33544456289522|2022-05-30|
    +--------------------+----------+-------------------+------------------+----------+


### 2) Определяем структуру хранения

* stage

    user/master/data/events

* ods

    не используем

* cdm

    т.к все витрины строятся на одном наборе данных - события с указанием города, то рационально было бы один раз собрать данную таблицу и положить в данном слое. Это облегчило бы расчет витрин. Но я пока пропущу данную "рационализацию". Сделаю, если останется время.

* marts

    user/andreydzr/data/analytics/mart1
    user/andreydzr/data/analytics/mart2
    user/andreydzr/data/analytics/mart3

* temp

    Данный путь будем использовать для разработки, тестов и т.п

    /user/andreydzr/test/

Данные будем хранить в формате .parquet. 
исходные данные партицированны по дням, но обновлять витрины инкрементно не получится, т.к используемые метрики требуют расчета по всей глубине данных. Поэтому пересчитывать витрины будем каждый день полностью.

### 3) Подготовка скриптов

Скрипт для витрины 1 см. src\scripts\mart_1.py

Т.к при тестировании скрипта работа произовится с неполной выборкой, то есть вероятность, что события в выборке не обеспечат выполнение условия задания и не будет таких пользователей, которые задерживались в одном городе более чем на 27 дней. Поэтому в коде установлено значение 10. При таком значении поле home_city в витрине будет иметь ненулевые значения с большей вероятностью. 

Скрипт запускаем следующей командой:

    spark-submit /lessons/dags/mart_1.py https://code.s3.yandex.net/data-analyst/data_engeneer/geo.csv 2022-06-21 60 /user/master/data/geo/events /user/andreydzr/test/mart_1

Проверяем результат:

    from pyspark.sql import SparkSession


    hdfs_url = 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020'

    spark = SparkSession.builder \
                    .appName('datareader') \
                    .master('local') \
                    .getOrCreate()

    spark.read.parquet(hdfs_url + '/user/andreydzr/test/mart_1').orderBy(F.desc('home_city')).show()

Витрина 1:
+-------+-----------+--------------------+-----------+------------+--------------------+
|user_id|   act_city|          local_time|  home_city|travel_count|  collect_list(city)|
+-------+-----------+--------------------+-----------+------------+--------------------+
|  95546| Townsville| 2021-06-08 12:43:58| Townsville|           1|        [Townsville]|
| 110023|     Sydney| 2021-06-10 06:46:06|     Sydney|           7|[Canberra, Sydney...|
|    625|Rockhampton|2021-05-15 04:21:...|Rockhampton|           4|[Rockhampton, Bun...|
| 158783|Rockhampton|2021-05-26 12:57:...|Rockhampton|           3|[Rockhampton, Gol...|
| 148319|  Melbourne| 2022-04-23 10:06:21|  Melbourne|           1|[Maitland, Melbou...|
| 105743|   Maitland|2021-06-05 20:16:...|   Maitland|           1|          [Maitland]|
|  23134|   Maitland| 2021-05-17 19:08:56|   Maitland|           3|[Maitland, Canber...|
|  19191|     Hobart|2021-05-23 00:50:...|    Bunbury|           5|[Bunbury, Bunbury...|
| 111641|   Adelaide|2021-06-16 08:10:...|   Adelaide|           6|[Brisbane, Adelai...|
|      5|Rockhampton| 2022-05-28 01:23:55|       null|           1|       [Rockhampton]|
|      9|   Canberra|2021-05-03 07:05:...|       null|           1|          [Canberra]|
|     24|  Newcastle|2021-05-11 13:27:...|       null|           1|         [Newcastle]|
|     54|     Darwin| 2022-04-25 17:49:36|       null|           1|            [Darwin]|
|     96|  Newcastle|2021-05-29 02:02:...|       null|           1|         [Newcastle]|
|     26|  Newcastle| 2022-05-13 11:12:06|       null|           1|         [Newcastle]|
|     30|     Mackay|2021-05-08 13:24:...|       null|           1|            [Mackay]|
|     55|   Maitland| 2022-05-27 23:26:24|       null|           1|          [Maitland]|
|     57|     Darwin|2021-05-09 18:04:...|       null|           1|            [Darwin]|
|     97|  Melbourne| 2022-04-25 02:06:28|       null|           1|         [Melbourne]|
|     43| Cranbourne|2021-05-11 14:47:...|       null|           1|        [Cranbourne]|
+-------+-----------+--------------------+-----------+------------+--------------------+

P.S.
local_time посчитан для всех пользователей по часовому поясу Sydney. При использовании в качестве tz названий городов (home_city) функция from_utc_timestamp() на некоторых городах упадет с ошибкой, т.к не распознает такую таймзону.


Скрипт для витрины 2 см. src\scripts\mart_2.py

Скрипт запускаем следующей командой:

    /usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster --conf spark.sql.broadcastTimeout=600s /lessons/dags/mart_2.py https://code.s3.yandex.net/data-analyst/data_engeneer/geo.csv 2022-06-21 60 /user/master/data/geo/events /user/andreydzr/test/mart_2

Проверяем результат:

    spark.read.parquet(hdfs_url + '/user/andreydzr/test/mart_2').orderBy('zone_id','month').show(30)

Витрина 2:
+-------------------+-------------------+-------+------------+-------------+-----------------+---------+-------------+--------------+------------------+----------+
|               week|              month|zone_id|week_message|week_reaction|week_subscription|week_user|month_message|month_reaction|month_subscription|month_user|
+-------------------+-------------------+-------+------------+-------------+-----------------+---------+-------------+--------------+------------------+----------+
|2022-04-18 00:00:00|2022-04-01 00:00:00|   null|           0|         8928|            16844|        0|            0|         38080|             71868|         0|
|2022-04-25 00:00:00|2022-04-01 00:00:00|   null|           0|        34339|            64928|        0|            0|         38080|             71868|         0|
|2022-05-23 00:00:00|2022-05-01 00:00:00|   null|           0|        83460|           161493|        0|            0|        252816|            496316|         0|
|2022-05-30 00:00:00|2022-05-01 00:00:00|   null|           0|        18327|            44562|        0|            0|        252816|            496316|         0|
|2022-05-16 00:00:00|2022-05-01 00:00:00|   null|           0|        58601|           113231|        0|            0|        252816|            496316|         0|
|2022-04-25 00:00:00|2022-05-01 00:00:00|   null|           0|        34339|            64928|        0|            0|        252816|            496316|         0|
|2022-05-02 00:00:00|2022-05-01 00:00:00|   null|           0|        39844|            76407|        0|            0|        252816|            496316|         0|
|2022-05-09 00:00:00|2022-05-01 00:00:00|   null|           0|        47397|            90719|        0|            0|        252816|            496316|         0|
|2021-04-19 00:00:00|2021-04-01 00:00:00|      1|          16|            0|                0|       15|           34|             0|                 0|        31|
|2021-04-26 00:00:00|2021-04-01 00:00:00|      1|          26|            0|                0|       22|           34|             0|                 0|        31|
|2021-05-10 00:00:00|2021-05-01 00:00:00|      1|           8|            0|                0|        6|           50|             0|                 0|        40|
|2021-04-26 00:00:00|2021-05-01 00:00:00|      1|          26|            0|                0|       22|           50|             0|                 0|        40|
|2021-05-03 00:00:00|2021-05-01 00:00:00|      1|          19|            0|                0|       16|           50|             0|                 0|        40|
|2021-05-24 00:00:00|2021-05-01 00:00:00|      1|           6|            0|                0|        4|           50|             0|                 0|        40|
|2021-05-17 00:00:00|2021-05-01 00:00:00|      1|           9|            0|                0|        8|           50|             0|                 0|        40|
|2021-06-07 00:00:00|2021-06-01 00:00:00|      1|           1|            0|                0|        0|            2|             0|                 0|         1|
|2021-05-31 00:00:00|2021-06-01 00:00:00|      1|           1|            0|                0|        1|            2|             0|                 0|         1|
|2022-04-18 00:00:00|2022-04-01 00:00:00|      1|           2|            6|               10|        2|            5|            14|                36|         5|
|2022-04-25 00:00:00|2022-04-01 00:00:00|      1|           3|           11|               34|        3|            5|            14|                36|         5|
|2022-05-16 00:00:00|2022-05-01 00:00:00|      1|           6|           32|               53|        6|           20|           145|               305|        20|
|2022-04-25 00:00:00|2022-05-01 00:00:00|      1|           3|           11|               34|        3|           20|           145|               305|        20|
|2022-05-23 00:00:00|2022-05-01 00:00:00|      1|           4|           48|              100|        4|           20|           145|               305|        20|
|2022-05-09 00:00:00|2022-05-01 00:00:00|      1|           3|           22|               64|        3|           20|           145|               305|        20|
|2022-05-02 00:00:00|2022-05-01 00:00:00|      1|           4|           29|               61|        4|           20|           145|               305|        20|
|2022-05-30 00:00:00|2022-05-01 00:00:00|      1|           3|           11|               19|        3|           20|           145|               305|        20|
|2021-04-26 00:00:00|2021-04-01 00:00:00|      2|          58|            0|                0|       32|           77|             0|                 0|        45|
|2021-04-19 00:00:00|2021-04-01 00:00:00|      2|          37|            0|                0|       24|           77|             0|                 0|        45|
|2021-04-26 00:00:00|2021-05-01 00:00:00|      2|          58|            0|                0|       32|          111|             0|                 0|        67|
|2021-05-31 00:00:00|2021-05-01 00:00:00|      2|           1|            0|                0|        1|          111|             0|                 0|        67|
|2021-05-24 00:00:00|2021-05-01 00:00:00|      2|           5|            0|                0|        2|          111|             0|                 0|        67|
+-------------------+-------------------+-------+------------+-------------+-----------------+---------+-------------+--------------+------------------+----------+


Скрипт для  витрины 3 см. src\scripts\mart_3.py

Т.к при тестировании скрипта работа произовится с неполной выборкой, то есть вероятность, что события в выборке не обеспечат выполнение условия задания и не будет таких пользователей, расстояние между последними сообщениями которых меньше 1 км. Поэтому в коде установлено значение 30:

    161       .where(F.col('distance') <= 30) 

Такое условие на тестовой выборке выполнится с большей вероятностью. 

Скрипт запускаем следующей командой:

    spark-submit /lessons/dags/mart_3.py https://code.s3.yandex.net/data-analyst/data_engeneer/geo.csv 2022-05-21 25 /user/master/data/geo/events /user/andreydzr/test/mart_3

Проверяем результат:

    spark.read.parquet(hdfs_url + '/user/andreydzr/test/mart_3').where(F.col('distance').isNotNull()).orderBy('distance').show()

Витрина 3 (Без вызова последних двух методов):
+---------+----------+------------------+--------+--------------------+--------+--------------------+--------------------+
|user_left|user_right|          distance|zone_id1|        local_time_1|zone_id2|        local_time_2|      processed_dttm|
+---------+----------+------------------+--------+--------------------+--------+--------------------+--------------------+
|    16051|    118800| 4.355435870318231|       8|2021-05-05 16:00:...|       8|2021-05-02 20:39:...|2022-12-22 16:33:...|
|   118800|     16051| 4.355435870318231|       8|2021-05-02 20:39:...|       8|2021-05-05 16:00:...|2022-12-22 16:33:...|
|   116710|      5977| 4.790753299762835|       4|2021-05-09 06:21:...|       4| 2021-05-12 09:21:22|2022-12-22 16:33:...|
|     5977|    116710| 4.790753299762835|       4| 2021-05-12 09:21:22|       4|2021-05-09 06:21:...|2022-12-22 16:33:...|
|     1854|     69226| 4.849541154615658|       3|2021-04-29 06:11:...|       3|2021-05-08 08:54:...|2022-12-22 16:33:...|
|    69226|      1854| 4.849541154615658|       3|2021-05-08 08:54:...|       3|2021-04-29 06:11:...|2022-12-22 16:33:...|
|   160186|     74990| 4.957710807216852|      20|2021-05-04 03:00:...|      12|2021-05-07 00:38:...|2022-12-22 16:33:...|
|    74990|    160186| 4.957710807216852|      12|2021-05-07 00:38:...|      20|2021-05-04 03:00:...|2022-12-22 16:33:...|
|   119526|      2718| 7.808419532295549|       2|2021-05-16 03:42:...|       2|2021-04-28 09:03:...|2022-12-22 16:33:...|
|     2718|    119526| 7.808419532295549|       2|2021-04-28 09:03:...|       2|2021-05-16 03:42:...|2022-12-22 16:33:...|
|    85982|    119691| 8.419619537567607|       2|2021-05-06 16:49:...|       2|2021-05-13 14:27:...|2022-12-22 16:33:...|
|   119691|     85982| 8.419619537567607|       2|2021-05-13 14:27:...|       2|2021-05-06 16:49:...|2022-12-22 16:33:...|
|   151481|     62893| 8.812335219913253|       2|2021-05-12 17:34:...|       2|2021-05-21 13:45:...|2022-12-22 16:33:...|
|    62893|    151481| 8.812335219913253|       2|2021-05-21 13:45:...|       2|2021-05-12 17:34:...|2022-12-22 16:33:...|
|   125289|     40565|  8.91496778479496|       3|2021-05-08 05:32:...|       3|2021-05-01 06:45:...|2022-12-22 16:33:...|
|    40565|    125289|  8.91496778479496|       3|2021-05-01 06:45:...|       3|2021-05-08 05:32:...|2022-12-22 16:33:...|
|      626|     65912| 9.854107995404158|       6|2021-05-08 14:17:...|       6|2021-05-11 20:50:...|2022-12-22 16:33:...|
|    65912|       626| 9.854107995404158|       6|2021-05-11 20:50:...|       6|2021-05-08 14:17:...|2022-12-22 16:33:...|
|   133672|     82708|10.088340463617628|      23| 2021-05-16 00:25:55|      23|2021-05-06 21:30:...|2022-12-22 16:33:...|
|    82708|    133672|10.088340463617628|      23|2021-05-06 21:30:...|      23| 2021-05-16 00:25:55|2022-12-22 16:33:...|
+---------+----------+------------------+--------+--------------------+--------+--------------------+--------------------+ 

Витрина 3 (Весь код полностью):
+---------+----------+--------------------+-------+--------------------+
|user_left|user_right|      processed_dttm|zone_id|          local_time|
+---------+----------+--------------------+-------+--------------------+
|   124586|     88595|2022-12-22 19:01:...|      9|2022-12-23 06:01:...|
|   135600|     71269|2022-12-22 19:01:...|      8|2022-12-23 06:01:...|
|    13768|     92081|2022-12-22 19:01:...|     12|2022-12-23 06:01:...|
|   142453|    155941|2022-12-22 19:01:...|      6|2022-12-23 06:01:...|
|    17283|     50315|2022-12-22 19:01:...|      2|2022-12-23 06:01:...|
|    28985|    158369|2022-12-22 19:01:...|      1|2022-12-23 06:01:...|
|    33821|    155273|2022-12-22 19:01:...|      3|2022-12-23 06:01:...|
|    40049|     14948|2022-12-22 19:01:...|   null|2022-12-23 06:01:...|
|    43367|     22148|2022-12-22 19:01:...|     22|2022-12-23 06:01:...|
|    44961|    141961|2022-12-22 19:01:...|      1|2022-12-23 06:01:...|
|    63129|    145694|2022-12-22 19:01:...|      4|2022-12-23 06:01:...|
|    76373|    156291|2022-12-22 19:01:...|      3|2022-12-23 06:01:...|
|    87425|     31839|2022-12-22 19:01:...|     23|2022-12-23 06:01:...|
|    93872|     29058|2022-12-22 19:01:...|     19|2022-12-23 06:01:...|
|   107984|     26210|2022-12-22 19:01:...|      1|2022-12-23 06:01:...|
|   131613|     32728|2022-12-22 19:01:...|     13|2022-12-23 06:01:...|
|   158369|     28985|2022-12-22 19:01:...|      1|2022-12-23 06:01:...|
|   159067|     34926|2022-12-22 19:01:...|      3|2022-12-23 06:01:...|
|    31564|    157471|2022-12-22 19:01:...|   null|2022-12-23 06:01:...|
|    67427|    124805|2022-12-22 19:01:...|   null|2022-12-23 06:01:...|
+---------+----------+--------------------+-------+--------------------+

P.S.
В некоторых случаях zone_id принимает значение null т.к левый и правый пользователь находятся в разных городах. Это связано с тем, что допущено расстояние между пользователями до 30 км (см выше).
local_time посчитан для всех пользователей по часовому поясу Sydney. При использовании в качестве tz названий городов функция from_utc_timestamp() на некоторых городах упадет с ошибкой, т.к не распознает такую таймзону.


### 3) Автоматизация запуска скриптов

см. src\dags\dag.py