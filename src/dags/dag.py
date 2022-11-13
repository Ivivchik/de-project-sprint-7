import pendulum
import logging

from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

TASK_LOGGER = logging.getLogger('hackathon')
EVENTS_PATH = '/user/ivichick/data/silver/events'
DIM_GEO_PATH = '/user/ivichick/data/dim_geo'
ANALYTICS_PATH = '/user/ivichick/analytics'

@dag(description='Provide project dag for sprint7',
     schedule_interval='@daily',
     start_date=pendulum.parse('2022-10-01'),
     catchup=False,
     tags=['project-spint-7']
     )

def recommend_dm():

    events_partitioned = SparkSubmitOperator(
        task_id='events_partitioned',
        application='/home/scripts/repartition.py',
        application_args=['{{ ds }}', '/user/master/data/geo/events', EVENTS_PATH],
    )

    dim_city = SparkSubmitOperator(
        task_id='dim_city',
        application='/home/scripts/read_csv_geo.py',
        application_args=['{{ ds }}', 'https://code.s3.yandex.net/data-analyst/data_engeneer/geo.csv', DIM_GEO_PATH],
    )

    user_city = SparkSubmitOperator(
        task_id='user_city',
        application='/home/scripts/user_city.py',
        application_args=['{{ ds }}', 30, ANALYTICS_PATH, DIM_GEO_PATH, EVENTS_PATH],
    )

    message_by_city = SparkSubmitOperator(
        task_id='message_by_city',
        application='/home/scripts/message_by_city.py',
        application_args=['{{ ds }}', 30, ANALYTICS_PATH, DIM_GEO_PATH, EVENTS_PATH],
    )

    recommendations = SparkSubmitOperator(
        task_id='recommendations.py',
        application='/home/scripts/recommendation.py',
        application_args=[ANALYTICS_PATH, DIM_GEO_PATH, EVENTS_PATH, '/user/ivichick/analytics/user_city'],
    )

    events_partitioned >> [dim_city, user_city, message_by_city] >> recommendations

_ = recommend_dm()