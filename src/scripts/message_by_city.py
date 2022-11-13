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
from functools import reduce
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from datetime import datetime, timedelta

EARTH_RADIUS = 6371

def input_paths(date, days_count, events_base_path):
    datetm = datetime.strptime(date, '%Y-%m-%d').date()
    res = [f'{events_base_path}/date={str(datetm - timedelta(days=x))}' for x in range(0, days_count)]
    return res

def message_by_city(message, cities):
    
    win_function = Window.partitionBy('message_id').orderBy('d')

    message_city = message.select(F.col('lat'), F.col('lon'),
                F.col('event').getItem('message_id').alias('message_id'),
                F.col('event').getItem('message_ts').alias('message_ts'),
                F.col('event').getItem('message_from').alias('user_id'),
                F.col('event_type'))\
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
        .select(F.col('zone_id'),F.col('event_type'),
                F.weekofyear(F.col('message_ts')).alias('week'),
                F.month(F.col('message_ts')).alias('month'))

    return message_city

def message_by_city_w(message_city):
    res = message_city.groupBy('week', 'zone_id')\
        .agg(F.sum(F.when(F.col('event_type') == 'message', 1).otherwise(0)).alias('week_message'),
    F.sum(F.when(F.col('event_type') == 'reaction', 1).otherwise(0)).alias('week_reaction'),
    F.sum(F.when(F.col('event_type') == 'subscription', 1).otherwise(0)).alias('week_subscription'),
    F.sum(F.when(F.col('event_type') == 'registartion', 1).otherwise(0)).alias('week_registartion'))\

    return res

def message_by_city_m(message_city):
    res = message_city.groupBy('month', 'zone_id')\
        .agg(F.sum(F.when(F.col('event_type') == 'message', 1).otherwise(0)).alias('month_message'),
    F.sum(F.when(F.col('event_type') == 'reaction', 1).otherwise(0)).alias('month_reaction'),
    F.sum(F.when(F.col('event_type') == 'subscription', 1).otherwise(0)).alias('month_subscription'),
    F.sum(F.when(F.col('event_type') == 'registartion', 1).otherwise(0)).alias('month_user'))\

    return res

def message_by_city_m_and_w(message_by_city_w_df, message_by_city_m_df, output_path, days_count):
    message_by_city_w_df\
        .join(message_by_city_m_df, ['zone_id'])\
        .repartition(1).write.mode("overwrite").parquet(f'{output_path}/message_city{days_count}')

def main():

    date = sys.argv[1]
    days_count = sys.argv[2]
    output_path = sys.argv[3]
    dim_geo_path = sys.argv[4]
    geo_event_path = sys.argv[5]

    spark = SparkSession.builder \
                        .master('local[*]') \
                        .appName(f'Message-by-city-{date}') \
                        .getOrCreate()

    paths = input_paths(date, days_count, geo_event_path)

    events = [spark.read.orc(x) for x in paths]
    df = reduce(lambda x, y: x.union(y), events)

    cities = spark.read.orc(dim_geo_path).select(F.col('id').alias('zone_id'), F.col('dim_lat'), F.col('dim_lon'))
    message_city = message_by_city(df, cities)
    message_by_city_w_df = message_by_city_w(message_city)
    message_by_city_m_df = message_by_city_m(message_city)

    message_by_city_m_and_w(message_by_city_w_df, message_by_city_m_df, output_path, days_count)

if __name__ == "__main__":
    main()