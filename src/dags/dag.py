import pendulum
import logging

from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

TASK_LOGGER = logging.getLogger('project-sprint-7')
EVENTS_PATH = '/user/ivichick/data/silver/events'
DIM_GEO_PATH = '/user/ivichick/data/silver/dim_geo'
ANALYTICS_PATH = '/user/ivichick/analytics'
SOURCE_PATH = '/user/master/data/geo/events'
URL_GEO = 'https://code.s3.yandex.net/data-analyst/data_engeneer/geo.csv'

@dag(description='Provide project dag for sprint7',
     schedule_interval='@daily',
     start_date=pendulum.parse('2022-05-31'),
     catchup=False,
     tags=['project-spint-7']
     )

def recommend_dm():

    dump = DummyOperator(
        task_id='dummy'
    )

    events_partitioned = SparkSubmitOperator(
        task_id='events_partitioned',
        application='/home/scripts/repartition.py',
        application_args=['{{ ds }}', SOURCE_PATH, EVENTS_PATH],
    )

    dim_city = SparkSubmitOperator(
        task_id='dim_city',
        application='/home/scripts/read_csv_geo.py',
        application_args=['{{ ds }}', URL_GEO, DIM_GEO_PATH],
    )

    user_city = SparkSubmitOperator(
        task_id='user_city',
        application='/home/scripts/user_city.py',
        application_args=['{{ ds }}', '30', ANALYTICS_PATH, DIM_GEO_PATH, EVENTS_PATH],
    )

    message_by_city = SparkSubmitOperator(
        task_id='message_by_city',
        application='/home/scripts/message_by_city.py',
        application_args=['{{ ds }}', '30', ANALYTICS_PATH, DIM_GEO_PATH, EVENTS_PATH],
    )

    recommendations = SparkSubmitOperator(
        task_id='recommendations.py',
        application='/home/scripts/recommendation.py',
        application_args=['{{ ds }}', ANALYTICS_PATH, DIM_GEO_PATH, EVENTS_PATH],
    )

    [events_partitioned, dim_city] >> dump >> [user_city, message_by_city] >> recommendations

_ = recommend_dm()