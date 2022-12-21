import os
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8' 

import findspark
findspark.init() 
findspark.find()

import sys
import pyspark.sql.functions as F

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window

EARTH_RADIUS = 6371

def spark_init(date: str) -> SparkSession:
    return (SparkSession.builder
                        .master('yarn')
                        .config('spark.executor.memory', '1G')
                        .config('spark.driver.memory', '1G')
                        .config('spark.executor.cores', 2)
                        .config('spark.executor.instances', 4)
                        .appName(f'Recommendations-{date}')
                        .getOrCreate())

def get_user_cnannel(events: DataFrame) -> DataFrame:

    return (events
        .filter(F.col('event_type') == 'subscription')
        .select(F.col('subscription_channel').alias('channel'),
                F.col('user').alias('user_id')))

def get_act_message(events: DataFrame) -> DataFrame:

    df = (events
            .filter(F.col('event_type') == 'message')
            .select(F.col('lat'),
                    F.col('lon'),
                    F.col('message_ts'),
                    F.col('message_from').alias('user_id'))
    )

    return (df
                .groupBy(F.col('user_id'))
                .agg(F.max(F.col('message_ts')).alias('message_ts'))
                .join(df, ['message_ts', 'user_id'])
                .select(F.col('user_id'), F.col('lat'),F.col('lon'))
    )



def calculate_message_around_km(message_1: DataFrame,
                                message_2: DataFrame) -> DataFrame:

    return (message_1.alias('m1')
        .crossJoin(message_2.alias('m2'))
        .filter(F.col('m1.user_id') != F.col('m2.user_id'))
        .withColumn('dlat', F.sin((F.radians(F.col('m1.lat'))-
                            F.radians(F.col('m2.lat'))) / 2)
        )
        .withColumn('dlon', F.sin((F.radians(F.col('m1.lon')) -
                            F.radians(F.col('m2.lon'))) / 2)
        )
        .withColumn('tmp', F.col('dlat') * F.col('dlat') +
                           F.col('dlon') * F.col('dlon') *
                           F.cos(F.radians(F.col('m1.lat'))) *
                           F.cos(F.radians(F.col('m2.lat')))
        )
        .withColumn('tmp1', F.asin(F.sqrt(F.col('tmp'))))
        .withColumn('distance', 2 * F.col('tmp1') * EARTH_RADIUS)
        .filter(F.col('distance') <= 1)
        .select(
           F.col('m1.user_id').alias('user_id_left'),
           F.col('m1.lat').alias('lat'),
           F.col('m1.lon').alias('lon'),
           F.col('m2.user_id').alias('user_id_right'),
        )
    )

def calculate_mutual_channel(message_around_km: DataFrame,
                             user_channel: DataFrame) -> DataFrame: 

    return (message_around_km
        .join(user_channel.alias('u1'), F.col('user_id_left') == F.col('u1.user_id'))
        .join(user_channel.alias('u2'), F.col('user_id_right') == F.col('u2.user_id'))
        .filter(F.col('u1.channel') == F.col('u2.channel'))
    )

def calculate_no_communicate_users(user_with_mutual_channel: DataFrame,
                                   all_communicate: DataFrame) -> DataFrame:

    return (user_with_mutual_channel
                .join(all_communicate, ['user_id_left', 'user_id_right'], 'left_anti')
    )

def calculate_all_communications(df: DataFrame) -> DataFrame:

    messages = df.filter(F.col('event_type') == 'message')

    message_from = messages.select(F.col('message_from'), F.col('message_to'))
    message_to = messages.select(F.col('message_to'), F.col('message_from'))

    return(message_from.union(message_to)
            .select(F.col('message_from').alias('user_id_left'),
                    F.col('message_to').alias('user_id_right'))
    )

def recommends(df: DataFrame, dim_geo: DataFrame, output_path: str) -> None:

    list_city = ['Sydney', 'Melbourne', 'Brisbane', 'Perth',
                 'Adelaide', 'Canberra', 'Hobart', 'Darwin'
    ]

    win_fun_message = Window.partitionBy('user_id_left').orderBy('distance')
    res = (df.crossJoin(dim_geo)
                .withColumn('dlat', F.sin((F.radians(F.col('lat')) -
                            F.radians(F.col('dim_lat'))) / 2)
                )
                .withColumn('dlon', F.sin((F.radians(F.col('lon')) -
                                    F.radians(F.col('dim_lon'))) / 2)
                )
                .withColumn('tmp', F.col('dlat') * F.col('dlat') +
                                   F.col('dlon') * F.col('dlon') *
                                   F.cos(F.radians(F.col('lat'))) *
                                   F.cos(F.radians(F.col('dim_lat')))
                )
                .withColumn('tmp1', F.asin(F.sqrt(F.col('tmp'))))
                .withColumn('distance', 2 * F.col('tmp1') * EARTH_RADIUS)
                .withColumn('near_d', F.row_number().over(win_fun_message))
                .filter(F.col('near_d') == 1)
                .withColumn('local_time', 
                            F.when(F.col('city').isin(list_city),
                                    F.from_utc_timestamp(
                                        F.col('message_ts'),
                                        F.concat_ws('/', F.lit('Australia'), F.col('city'))
                                    )
                            )
                )
                .select(F.col('user_id_left'),
                        F.col('user_id_right'),
                        F.col('zone_id'),
                        F.current_timestamp().alias('processed_dttm'),
                        F.col('local_time')
                )
    )

    res.repartition(1).write.orc(f'{output_path}/recommend')


def main():


    date = sys.argv[1]
    output_path = sys.argv[2]
    dim_geo_path = sys.argv[3]
    events_path = sys.argv[4]

    spark = spark_init(date)

    events = spark.read.orc(events_path)
    dim_geo = (spark.read.orc(dim_geo_path)
                            .select(F.col('id').alias('zone_id'),
                                    F.col('city'),
                                    F.col('dim_lat'),
                                    F.col('dim_lon'))
    )

    users_cnannel = get_user_cnannel(events)
    message_1 = get_act_message(events)
    message_2 = message_1
    message_around_km = calculate_message_around_km(message_1, message_2)
    mutual_channel = calculate_mutual_channel(message_around_km, users_cnannel)
    all_communications = calculate_all_communications(events)
    no_communicate_users = calculate_no_communicate_users(mutual_channel, all_communications)

    recommends(no_communicate_users, dim_geo, output_path)


if __name__ == "__main__":
    main()