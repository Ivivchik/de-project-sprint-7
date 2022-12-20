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

from pyspark import SparkFiles
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.sql import SparkSession


def main():

    date = sys.argv[1]
    dim_geo_url = sys.argv[2]
    output_base_path = sys.argv[3]

    spark = (SparkSession.builder
                        .master('yarn')
                        .config('spark.executor.memory', '1G')
                        .config('spark.driver.memory', '1G')
                        .config('spark.executor.cores', 2)
                        .config('spark.executor.instances', 4)
                        .appName(f'Read_file_from_url-{date}') 
                        .config("spark.files.overwrite", "true")
                        .getOrCreate()
    )

    spark.sparkContext.addFile(dim_geo_url)

    src = spark.read.options(header=True, delimiter=';', inferSchema='True')\
               .csv("file://" + SparkFiles.get('geo.csv'))

    src.select(F.col('id').cast(IntegerType()),
               F.col('city'),
               F.regexp_replace(F.col('lat'), ',', '.').cast(DoubleType()).alias('dim_lat'),
               F.regexp_replace(F.col('lng'), ',', '.').cast(DoubleType()).alias('dim_lon'))\
       .repartition(1).write.mode('overwrite').orc(f'{output_base_path}')

if __name__ == "__main__":
    main()