from pyspark.sql import SparkSession, DataFrame

class CommonUtils():

    def spark_init(name: str, date: str) -> DataFrame:
        return (SparkSession.builder
                    .master('local[*]')
                    .appName(f'{name}-{date}')
                    .getOrCreate())
