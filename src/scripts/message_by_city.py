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

from pyspark.sql.types import *
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
from datetime import datetime, timedelta

EARTH_RADIUS = 6371

def spark_init(date: str) -> SparkSession:
    return (SparkSession.builder
                        .master('yarn')
                        .config('spark.executor.memory', '1G')
                        .config('spark.driver.memory', '1G')
                        .config('spark.executor.cores', 2)
                        .config('spark.executor.instances', 4)
                        .appName(f'Message-by-city-{date}')
                        .getOrCreate())

def message_by_city(events: DataFrame, dim_geo: DataFrame) -> DataFrame:
    
    win_fun_message_d = Window.partitionBy('message_id').orderBy('distance')
    win_fun_first_message = Window.partitionBy('user_id').orderBy('message_ts')

    events_by_city = (events
        .select(F.col('lat'),
                F.col('lon'),
                F.col('message_id'),
                F.col('message_ts'),
                F.col('message_from').alias('user_id'),
                F.col('event_type'))
        .crossJoin(dim_geo)
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
        .withColumn('near_d', F.row_number().over(win_fun_message_d))
        .filter(F.col('near_d') == 1)
        .withColumn('rn_message', F.row_number().over(win_fun_first_message))
        .withColumn('event_type', F.when(F.col('rn_message') == 1, 'registartion').otherwise(F.col('event_type')))
        .select(F.col('zone_id'),
                F.col('event_type'),
                F.date_trunc('month', F.col('message_ts')).alias('month'),
                F.date_trunc('week', F.col('message_ts')).alias('week')))

    return events_by_city

def message_by_city_w(message_city: DataFrame) -> DataFrame:

    return (message_city.groupBy('zone_id', 'month', 'week')
        .agg(F.sum(F.when(F.col('event_type') == 'message', 1).otherwise(0)).alias('week_message'),
    F.sum(F.when(F.col('event_type') == 'reaction', 1).otherwise(0)).alias('week_reaction'),
    F.sum(F.when(F.col('event_type') == 'subscription', 1).otherwise(0)).alias('week_subscription'),
    F.sum(F.when(F.col('event_type') == 'registartion', 1).otherwise(0)).alias('week_registartion')))


def message_by_city_m(message_city: DataFrame) -> DataFrame:
    
    return (message_city.groupBy('month', 'zone_id')
        .agg(F.sum(F.when(F.col('event_type') == 'message', 1).otherwise(0)).alias('month_message'),
    F.sum(F.when(F.col('event_type') == 'reaction', 1).otherwise(0)).alias('month_reaction'),
    F.sum(F.when(F.col('event_type') == 'subscription', 1).otherwise(0)).alias('month_subscription'),
    F.sum(F.when(F.col('event_type') == 'registartion', 1).otherwise(0)).alias('month_user')))

def message_by_city_m_and_w(message_by_city_w_df: DataFrame, 
                            message_by_city_m_df: DataFrame,
                            output_path: str,
                            days_count: int) -> None:
    (message_by_city_w_df
        .join(message_by_city_m_df, ['zone_id', 'month'])
        .repartition(1).write.mode("overwrite").parquet(f'{output_path}/message_city{days_count}'))

def main():

    date = sys.argv[1]
    days_count = int(sys.argv[2])
    output_path = sys.argv[3]
    dim_geo_path = sys.argv[4]
    events_path = sys.argv[5]

    spark = spark_init(date)
    datetm = datetime.strptime(date, '%Y-%m-%d').date()
    date_subs = str(datetm - timedelta(days=days_count))
    
    events = (spark.read.orc(events_path)
                        .filter(F.col('date').between(date_subs, datetm))
    )

    dim_geo = (spark.read.orc(dim_geo_path)
                    .select(F.col('id').alias('zone_id'),
                            F.col('dim_lat'),
                            F.col('dim_lon'))
    )

    message_city = message_by_city(events, dim_geo)
    message_by_city_w_df = message_by_city_w(message_city)
    message_by_city_m_df = message_by_city_m(message_city)

    message_by_city_m_and_w(message_by_city_w_df, message_by_city_m_df, output_path, days_count)

if __name__ == "__main__":
    main()