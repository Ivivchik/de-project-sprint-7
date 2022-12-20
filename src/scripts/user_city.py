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

from pyspark.sql.window import Window
from datetime import datetime,timedelta
from pyspark.sql import SparkSession, DataFrame

EARTH_RADIUS = 6371


def spark_init(date: str) -> SparkSession:
    return (SparkSession.builder
                        .master('yarn')
                        .config('spark.executor.memory', '1G')
                        .config('spark.driver.memory', '1G')
                        .config('spark.executor.cores', 2)
                        .config('spark.executor.instances', 4)
                        .appName(f'User-city-{date}')
                        .getOrCreate())

def get_cities(df: DataFrame) -> DataFrame:

    win_fun_user = Window.partitionBy('user_id').orderBy('message_ts')
    win_fun_user_city = Window.partitionBy('user_id', 'city').orderBy('message_ts')
    
    return (df
            .withColumn('rn1', F.row_number().over(win_fun_user))
            .withColumn('rn2', F.row_number().over(win_fun_user_city))
            .withColumn('seq', F.col('rn1') - F.col('rn2'))
            .groupBy(F.col('user_id'), F.col('city'), F.col('seq'))
            .agg(F.count(F.col('message_id')).alias('count_message'),
                 F.max(F.col('message_ts')).alias('message_ts'))
    )

def find_city(df: DataFrame, city_type: str) -> DataFrame:

    # Добавил этот лист так как в java остальных городов не time_zone.
    # Например Australia/Maitland
    # При подаче его в функцию from_utc_timestamp вылезает exception
    list_city = ['Sydney', 'Melbourne', 'Brisbane', 'Perth',
                 'Adelaide', 'Canberra', 'Hobart', 'Darwin'
    ]

    return (df
        .groupBy(F.col('user_id'))
        .agg(max(F.col('message_ts')).alias('message_ts'))
        .join(df, ['message_ts', 'user_id'])
        .withColumn('local_time', 
            F.when(F.col('city').isin(list_city),
                F.from_utc_timestamp(
                    F.col('message_ts'),
                    F.concat_ws('/', F.lit('Australia'), F.col('city'))
                )
            )
        )
        .select(F.col('user_id'),
                F.col('city').alias(city_type),
                F.col('local_time'))
    )

def calculate_count_travel_cities(df: DataFrame) -> DataFrame:
    
    return (df
        .groupBy(F.col('user_id'))
        .agg(F.count(F.col('city')).alias('travel_count'))
    )

def travel_cities_array(df: DataFrame) -> DataFrame:

    return (df
        .groupBy(F.col('user_id'))
        .agg(F.sort_array(
                F.collect_list(
                    F.struct(F.col('message_ts'), F.col('city'))
                )
            ).alias('arr_city')
        )
        .select(F.col('user_id'),
                F.col('arr_city').getItem('city').alias('travel_array'))
    )

def get_messages_by_city(messages: DataFrame, dim_geo: DataFrame) -> DataFrame:
    
    win_fun_message = Window.partitionBy('message_id').orderBy('distance')
    
    messages_by_city = (messages
        .select(F.col('lat'),
                F.col('lon'),
                F.col('message_id'),
                F.col('message_ts'),
                F.col('message_from').alias('user_id')
        )
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
        .withColumn('near_d', F.row_number().over(win_fun_message))
        .filter(F.col('near_d') == 1)
        .select(F.col('message_id'),
                F.col('user_id'),
                F.col('city'),
                F.col('message_ts'))
        )

    return messages_by_city

def user_city(user_cities: DataFrame,
              actual_city: DataFrame,
              home_city: DataFrame,
              count_travel_city: DataFrame,
              travel_cities: DataFrame,
              output_path: str,
              days_count: int) -> None:

    (user_cities
        .groupBy(F.col('user_id'))
        .distinct()
        .join(actual_city, ['user_id'], 'left')
        .join(home_city, ['user_id'], 'left')
        .join(count_travel_city, ['user_id'], 'left')
        .join(travel_cities, ['user_id'], 'left')
        .select(F.col('user_id'),
                F.col('act_city'),
                F.col('home_city'),
                F.col('travel_count'),
                F.col('travel_array'),
                actual_city['local_time'])
        .repartition(1)
        .write
        .mode("overwrite")
        .orc(f'{output_path}/user_city{days_count}')
    )


def main():

    date = sys.argv[1]
    days_count = int(sys.argv[2])
    output_path = sys.argv[3]
    dim_geo_path = sys.argv[4]
    events_path = sys.argv[5]

    spark = spark_init(date)
    datetm = datetime.strptime(date, '%Y-%m-%d').date()
    date_subs = str(datetm - timedelta(days=days_count))
    
    messages = (spark.read.orc(events_path)
                        .filter((F.col('date').between(date_subs, datetm)) &
                                (F.col('evet_type') == 'message')))

    dim_geo = spark.read.orc(dim_geo_path).select('city', 'dim_lat', 'dim_lon')
    messages_by_city = get_messages_by_city(messages, dim_geo)

    user_cities = get_cities(messages_by_city)

    home_city = find_city(user_cities, 'home_city')
    actual_city = find_city(user_cities, 'act_city')
    count_travel_city = calculate_count_travel_cities(user_cities)
    travel_cities = travel_cities_array(user_cities)

    user_city(user_cities,
              actual_city,
              home_city,
              count_travel_city,
              travel_cities,
              output_path,
              days_count)

if __name__ == "__main__":
    main()

