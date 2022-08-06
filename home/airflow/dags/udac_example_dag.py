from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='stage_events',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credentials = "aws_credentials",
    table = "staging_events",
    create_sql = SqlQueries.staging_events_create,
    s3_bucket = "udacity-dend",
    s3_key = "log_data",
    json_format = "s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='stage_songs',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credentials = "aws_credentials",
    table = "staging_songs",
    create_sql = SqlQueries.staging_songs_create,
    s3_bucket = "udacity-dend",
    s3_key = "song_data/A/A/A"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table = "songplays",
    create_sql = SqlQueries.songplays_create,
    select_sql = SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table = "users",
    create_sql = SqlQueries.users_create,
    select_sql = SqlQueries.user_table_insert,
    trunc_flag = True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table = "songs",
    create_sql = SqlQueries.songs_create,
    select_sql = SqlQueries.song_table_insert,
    trunc_flag = True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table = "artists",
    create_sql = SqlQueries.artists_create,
    select_sql = SqlQueries.artist_table_insert,
    trunc_flag = True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table = "time",
    create_sql = SqlQueries.time_create,
    select_sql = SqlQueries.time_table_insert,
    trunc_flag = True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    tables = ['songplays', 'songs', 'users', 'artists', 'time'],
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
