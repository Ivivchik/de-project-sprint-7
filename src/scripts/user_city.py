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

from math import pi
from datetime import datetime,timedelta
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

EARTH_RADIUS = 6371

def input_paths(date, days_count, events_base_path, event_type):
    datetm = datetime.strptime(date, '%Y-%m-%d').date()
    res = [f'{events_base_path}/date={str(datetm - timedelta(days=x))}/event_type={event_type}' for x in range(0, days_count)]
    return res


def actual_city(df):

    actual_city = df\
        .groupBy('user_id')\
        .agg(F.max('message_ts').alias('message_ts'))\
        .join(df, ['message_ts', 'user_id'])\
        .select(F.col('user_id'), F.col('city').alias('act_city'))

    return actual_city

def home_city(df):

    home_city = df\
        .groupBy('user_id', 'city')\
        .agg(F.countDistinct('message_id').alias('cnt_m'),
             F.max('message_ts').alias('message_ts'))\
        .filter(F.col('cnt_m') > 27)\
        .select(F.col('user_id'), F.col('city').alias('home_city'))

    return home_city

def travel_count(df):

    travel_count = df\
        .groupBy('user_id')\
        .agg(F.count('city').alias('travel_count'))

    return travel_count

def travel_array(df):

    travel_array = df\
        .groupBy('user_id')\
        .agg(F.sort_array(F.collect_list(F.struct("message_ts", "city"))).alias('arr_city'))\
        .select(F.col('user_id'), F.col('arr_city').getItem('city').alias('travel_array'))

    return travel_array

def message_by_city(message, cities):
    
    win_function = Window.partitionBy('message_id').orderBy('d')
    # Добавил этот лист так как в java остальных городов не time_zone.
    # Например Australia/Maitland
    # При подаче его в функцию from_utc_timestamp вылезает exception
    list_city = ['Sydney', 'Melbourne', 'Brisbane', 'Perth', 'Adelaide', 'Canberra', 'Hobart', 'Darwin']

    message_city = message\
        .select(F.col('lat'), F.col('lon'),
                F.col('event').getItem('message_id').alias('message_id'),
                F.col('event').getItem('message_ts').alias('message_ts'),
                F.col('event').getItem('message_from').alias('user_id'))\
        .crossJoin(cities)\
        .withColumn('lat_rad', F.col('lat') * pi / 180)\
        .withColumn('lon_rad', F.col('lon') * pi / 180)\
        .withColumn('dim_lat_rad', F.col('dim_lat') * pi / 180)\
        .withColumn('dim_lon_rad', F.col('dim_lon') * pi / 180)\
        .withColumn('d', 2 * EARTH_RADIUS * F.asin(F.sqrt(F.sin((F.col('dim_lat_rad') - F.col('lat_rad')) / 2) * F.sin((F.col('dim_lat_rad') - F.col('lat_rad')) / 2) + 
                                                  F.sin((F.col('dim_lon_rad') - F.col('lon_rad')) / 2) * F.sin((F.col('dim_lon_rad') - F.col('lon_rad')) / 2) *
                                                  F.cos(F.col('lat_rad')) * F.cos(F.col('dim_lat_rad')))))\
        .withColumn('near_d', F.row_number().over(win_function))\
        .filter(F.col('near_d') == 1)\
        .withColumn('local_time', F.when(F.col('city').isin(list_city),
                     F.from_utc_timestamp(F.col('message_ts'), F.concat_ws('/', F.lit('Australia'), F.col('city')))))\
        .select(F.col('message_id'), F.col('user_id'), F.col('city'),
                F.col('local_time'), F.col('message_ts'))

    return message_city

def user_city(message_city,
              actual_city,
              home_city,
              travel_count,
              travel_array,
              output_path,
              days_count):

    message_city\
        .join(actual_city, ['user_id'])\
        .join(home_city, ['user_id'], 'left')\
        .join(travel_count, ['user_id'], 'left')\
        .join(travel_array, ['user_id'], 'left')\
        .select('user_id', 'act_city', 'home_city', 'travel_count',
                'travel_array', 'message_id', 'city', 'local_time')\
        .repartition(1).write.mode("overwrite").parquet(f'{output_path}/user_city{days_count}')


def main():

    date = sys.argv[1]
    days_count = sys.argv[2]
    output_path = sys.argv[3]
    dim_geo_path = sys.argv[4]
    geo_event_path = sys.argv[5]


    spark = SparkSession.builder \
                        .master('local[*]') \
                        .appName(f'User_city-{date}') \
                        .config("spark.files.overwrite", "true")\
                        .getOrCreate()
    
    paths = input_paths(date, days_count, geo_event_path, 'message')
    message = spark.read.orc(*paths)
    cities = spark.read.orc(dim_geo_path).select('city', 'dim_lat', 'dim_lon')
    
    message_city = message_by_city(message, cities)
    user_actual_city = actual_city(message_city)
    user_home_city = home_city(message_city)
    user_travel_count = travel_count(message_city)
    user_travel_array = travel_array(message_city)

    user_city(message_city,
              user_actual_city,
              user_home_city,
              user_travel_count,
              user_travel_array,
              output_path,
              days_count)

if __name__ == "__main__":
    main()

