# Проект 7-го спринта
### Задание
В продукт планируют внедрить систему рекомендации друзей. Приложение будет предлагать пользователю написать человеку, если пользователь и адресат:
* состоят в одном канале,
* раньше никогда не переписывались,
* находятся не дальше 1 км друг от друга.

При этом команда хочет лучше изучить аудиторию соцсети, чтобы в будущем запустить монетизацию. Для этого было решено провести геоаналитику:
* Выяснить, где находится большинство пользователей по количеству сообщений, лайков и подписок из одной точки.
* Посмотреть, в какой точке Австралии регистрируется больше всего новых пользователей.
* Определить, как часто пользователи путешествуют и какие города выбирают.

таблица событий находится в HDFS по этому пути: /user/master/data/geo/events.
Координаты городов Австралии, которые аналитики собрали в одну таблицу, находятся в файле geo.csv. 

Необходимо создать три витрины:
Витрина 1
* user_id — идентификатор пользователя.
* act_city — актуальный адрес. Это город, из которого было отправлено последнее сообщение.
* home_city — домашний адрес. Это последний город, в котором пользователь был дольше 27 дней.
* travel_count — количество посещённых городов. Если пользователь побывал в каком-то городе повторно, то это считается за отдельное посещение.
* travel_array — список городов в порядке посещения.
* local_time - местное время

Витрина 2
* month — месяц расчёта;
* week — неделя расчёта;
* zone_id — идентификатор зоны (города);
* week_message — количество сообщений за неделю;
* week_reaction — количество реакций за неделю;
* week_subscription — количество подписок за неделю;
* week_user — количество регистраций за неделю;
* month_message — количество сообщений за месяц;
* month_reaction — количество реакций за месяц;
* month_subscription — количество подписок за месяц;
* month_user — количество регистраций за месяц.

Витрина 3(рекомендации друзей):
* user_left — первый пользователь;
* user_right — второй пользователь;
* processed_dttm — дата расчёта витрины;
* zone_id — идентификатор зоны (города);
* local_time — локальное время.

### Как запустить контейнер

1. Скопируйте репозиторий на свой компьютер. В качестве пароля укажите ваш `Access Token`, который нужно получить на странице [Personal Access Tokens](https://github.com/settings/tokens)):
	* `git clone https://github.com/{{ username }}/de-project-sprint-7.git`
2. Перейдите в директорию с проектом: 
	* `cd de-project-sprint-7`
3. Выполните проект и сохраните получившийся код в локальном репозитории:
	* `git add .`
	* `git commit -m 'my best commit'`
4. Обновите репозиторий в вашем GutHub-аккаунте:
	* `git push origin main`

### Структура репозитория

Внутри `src` расположены две папки:
- `/src/dags`;
- `/src/sql`.





stage
user/master/data/events
ods
user/andreydzr/data/events
user/andreydzr/data/cities
marts
user/andreydzr/data/analytics

spark-submit --master yarn --deploy-mode cluster /lessons/dags/cities.py https://code.s3.yandex.net/data-analyst/data_engeneer/geo.csv /user/andreydzr/data/cities
spark-submit /lessons/dags/cities.py https://code.s3.yandex.net/data-analyst/data_engeneer/geo.csv /user/andreydzr/data/cities
spark-submit /lessons/dags/mart_1.py

spark-submit /lessons/dags/mart_1.py https://code.s3.yandex.net/data-analyst/data_engeneer/geo.csv ; 2022-05-21 3 /user/master/data/geo/events /user/andreydzr/test/mart_1

spark-submit /lessons/dags/mart_3.py 2022-05-21 1 /user/master/data/geo/events /user/andreydzr/test/mart_3

/usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster /lessons/dags/mart_3.py 2022-05-21 1 /user/master/data/geo/events /user/andreydzr/test/mart_3


    # return users_pair \
    #         .join(user_coordinates, users_pair.user_left == user_coordinates.message_from, 'left') \
    #         .withColumnRenamed('message_from', 'lu') \
    #         .withColumnRenamed('act_lat', 'lat1') \
    #         .withColumnRenamed('act_lon', 'lng1') \
    #         .join(user_coordinates, users_pair.user_right == user_coordinates.message_from, 'left') \
    #         .withColumnRenamed('message_from', 'ru') \
    #         .withColumnRenamed('act_lat', 'lat2') \
    #         .withColumnRenamed('act_lon', 'lng2') \
    #         .withColumn(
    #             'distance',
    #                     F.lit(2)*F.lit(6371)*F.asin(
    #                             F.sqrt(
    #                                 F.pow(
    #                                     F.sin(
    #                                         (F.col('lat2') - F.col('lat1'))/F.lit(2)
    #                                     ), 2)\
    #                                 + F.cos('lat1')\
    #                                 * F.cos('lat2')\
    #                                 * F.pow(
    #                                     F.sin(
    #                                         (F.col('lng2')-F.col('lng1'))/F.lit(2)
    #                                     ) ,2)
    #                             )
    #                     )
    #         )\
    #         .select(
    #             'user_left',
    #             'user_right',
    #             'distance',
    #             F.current_timestamp().alias('processed_dttm'),
    #         )
            # .where(F.col('distance') < 10) \
#           F.from_utc_timestamp('max(ts)', 'Australia/Sydney').alias('local_time')