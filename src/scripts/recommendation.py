import os
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8' 

import findspark
findspark.init() 
findspark.find ()

import sys
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

EARTH_RADIUS = 6371
LIST_CITY = ['Sydney', 'Melbourne', 'Brisbane', 'Perth', 'Adelaide', 'Canberra', 'Hobart', 'Darwin']

def user_cnannel(events):

    users_cnannel = events\
        .filter(F.col('event_type') == 'subscription')\
        .select(F.col('event.subscription_channel').alias('channel'),
                F.col('event.user').alias('user'))

    return users_cnannel

def non_communicte_user(events):

    users = events\
        .filter(F.col('event_type') == 'message')\
        .select(F.col('event.message_from').alias('user_1'),
                F.col('event.message_to').alias('user_2'))

    combination_users = users.alias('users_1')\
        .join(users.alias('users_2'), 
                (F.col('users_1.user_1') != F.col('users_2.user_1')) &
                (F.col('users_1.user_1') != F.col('users_2.user_2')) &
                (F.col('users_1.user_2') != F.col('users_2.user_1')),'full')

    combination_users_from = combination_users\
        .select(F.col('users_1.user_1').alias('user'),
                F.col('users_2.user_1').alias('user_1'),
                F.col('users_2.user_2').alias('user_2'))
    
    combination_users_to = combination_users\
        .select(F.col('users_1.user_2').alias('user'),
                F.col('users_2.user_1').alias('user_1'),
                F.col('users_2.user_2').alias('user_2'))

    combination_users_from_to = combination_users_from\
        .unionByName(combination_users_to)\
        .groupBy('user')\
        .agg(F.array_distinct(F.concat(F.collect_set(F.col('user_1')),F.collect_set(F.col('user_2')))).alias('usr_comb'))\
        .distinct()

    communication_users_from_to = users.select(F.col('user_1'),F.col('user_2'))\
        .union(users.select(F.col('user_2'),F.col('user_1')))\
        .groupBy(F.col('user_1').alias('user')).agg(F.collect_set(F.col('user_2')).alias('usr_comm'))\
        .distinct()
    
    result = communication_users_from_to\
        .join(combination_users_from_to, ['user'])\
        .withColumn('non_comm', F.array_except(F.col('usr_comb'), F.col('usr_comm')))\
        .select(F.col('user'), F.explode(F.col('non_comm')).alias('user_comm'))\
        .withColumn('tmp', F.array_sort(F.array(F.col('user'), F.col('user_comm'))))\
        .select(F.col('tmp')[0].alias('user_left'), F.col('tmp')[1].alias('user_right')).distinct()\

    return result

def recommends(users_channel, non_communicte_users, users_city, dim_city, output_path):

    non_communicte_users\
        .join(users_channel.alias('c1'), F.col('user_left') == F.col('c1.user'))\
        .join(users_channel.alias('c2'), F.col('user_right') == F.col('c2.user'))\
        .join(users_city.alias('u1'), F.col('user_left') == F.col('u1.user_id'))\
        .join(users_city.alias('u2'), F.col('user_right') == F.col('u2.user_id'))\
        .filter(F.col('c1.channel') == F.col('c2.channel') &
                F.col('u1.home_city') == F.col('u2.home_city'))\
        .join(dim_city, F.col('u2.home_city') == F.col('city'))\
        .withColumn('processed_dttm', F.current_timestamp())\
        .withColumn('local_time', F.when(F.col('city').isin(LIST_CITY),
                     F.from_utc_timestamp(F.col('processed_dttm'), F.concat_ws('/', F.lit('Australia'), F.col('city')))))\
        .select(F.col('user_left'), F.col('user_right'), F.col('zone_id'),
                F.col('processed_dttm'), F.col('local_time'))\
        .repartition(1).write.mode("overwrite").parquet(f'{output_path}/recommendation')
        


def main():

    output_path = sys.argv[1]
    dim_geo_path = sys.argv[2]
    input_events_path = sys.argv[3]
    input_user_city_path = sys.argv[4]


    spark = SparkSession.builder \
                        .master('local[*]') \
                        .appName(f'Recommendations') \
                        .getOrCreate()

    events = spark.read.orc(input_events_path)
    dim_cities = spark.read.orc(dim_geo_path).select(F.col('id').alias('zone_id'), F.col('city'))
    users_city = spark.read.orc(input_user_city_path)
    users_cnannel = user_cnannel(events)
    non_communicte_users = non_communicte_user(events)

    recommends(users_cnannel, non_communicte_users, users_city, dim_cities, output_path)

if __name__ == "__main__":
    main()