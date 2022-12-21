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

def main():
    date = sys.argv[1]
    geo_events_base_path = sys.argv[2]
    output_base_path = sys.argv[3]

    spark = (SparkSession.builder
                        .master('local')
                        .appName(f'Repartition-{date}')
                        .getOrCreate())

    src = (spark.read.parquet(geo_events_base_path)
                .filter(F.col('date') >= date)
                .select('event.admins',
                        'event.channel_id',
                        'event.datetime',
                        'event.media',
                        'event.message',
                        'event.message_channel_to',
                        'event.message_from',
                        'event.message_group',
                        'event.message_id',
                        'event.message_to',
                        'event.message_ts',
                        'event.reaction_from',
                        'event.reaction_type',
                        'event.subscription_channel',
                        'event.subscription_user',
                        'event.tags',
                        'event.user',
                        'event_type',
                        'lat',
                        'lon',
                        'date')
    )

    (src
        .repartition(1)
        .write
        .partitionBy('date', 'event_type')
        .mode('overwrite')
        .orc(output_base_path))


if __name__ == "__main__":
    main()