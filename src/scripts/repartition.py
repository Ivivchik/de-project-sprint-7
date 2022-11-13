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
from pyspark.sql import SparkSession

def main():
    date = sys.argv[1]
    geo_events_base_path = sys.argv[2]
    output_base_path = sys.argv[3]

    spark = SparkSession.builder \
                        .master('yarn') \
                        .appName(f'Repartition-{date}') \
                        .getOrCreate()

    src = spark.read.parquet(f'{geo_events_base_path}/date={date}')
    src.repartition(1).write.partitionBy('event_type').mode('overwrite').orc(f'{output_base_path}/date={date}')


if __name__ == "__main__":
    main()