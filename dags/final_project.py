"""Project DAG.

This dag runs the data warehouse operation for sparkify.
"""

from datetime import timedelta
import pendulum
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from plugins.operators.stage_redshift import StageToRedshiftOperator
from plugins.operators.stage_redshift import LoadFactOperator
from plugins.operators.stage_redshift import LoadDimensionOperator
from plugins.operators.stage_redshift import DataQualityOperator
from plugins.helpers import sql_queries

default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    'schedule_interval': "@hourly",
    'catchup': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'depends_on_past': False,
}


@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow'
)
def final_project():
    """Execute Final project DAG."""
    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id='aws_redshift_connection',
        aws_credentials_id='aws_credentials',
        table='staging_events',
        s3_bucket='udacity-airflow-gb',
        s3_key='log-data',
        json_path='log_json_path.json',
        region='us-east-1',
        create_table=True,
        create_query=sql_queries.SqlQueries.staging_events_table_create,
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id='aws_redshift_connection',
        aws_credentials_id='aws_credentials',
        table='staging_songs',
        s3_bucket='udacity-airflow-gb',
        s3_key='song-data',
        json_path='auto',
        region='us-east-1',
        create_table=True,
        create_query=sql_queries.SqlQueries.staging_songs_table_create,
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id='aws_redshift_connection',
        table='songplays',
        sql_statement=sql_queries.SqlQueries.songplay_table_insert,
        create_table=True,
        create_query=sql_queries.SqlQueries.songplay_create,
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id='aws_redshift_connection',
        sql_statement=sql_queries.SqlQueries.user_table_insert,
        table='users',
        append=True,
        create_table=True,
        create_query=sql_queries.SqlQueries.user_create,
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id='aws_redshift_connection',
        sql_statement=sql_queries.SqlQueries.song_table_insert,
        table='songs',
        append=True,
        create_table=True,
        create_query=sql_queries.SqlQueries.songs_create,
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id='aws_redshift_connection',
        sql_statement=sql_queries.SqlQueries.artist_table_insert,
        table='artists',
        append=True,
        create_table=True,
        create_query=sql_queries.SqlQueries.artists_create,
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id='aws_redshift_connection',
        sql_statement=sql_queries.SqlQueries.time_table_insert,
        table='time',
        append=True,
        create_table=True,
        create_query=sql_queries.SqlQueries.time_create,
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        sql_queries=[sql_queries.SqlQueries.data_quality_user_id_nulls,
                     sql_queries.SqlQueries.data_quality_artist_id_nulls,
                     sql_queries.SqlQueries.data_quality_song_year],
        expected_results=[0, 0, 0],
        redshift_conn_id='aws_redshift_connection',
    )

    end_operator = DummyOperator(task_id='End_execution')

    start_operator >> [stage_songs_to_redshift, stage_events_to_redshift]
    [stage_songs_to_redshift, stage_events_to_redshift] >> load_songplays_table
    load_songplays_table >> [load_user_dimension_table,
                             load_song_dimension_table,
                             load_time_dimension_table,
                             load_artist_dimension_table]
    [load_user_dimension_table,
     load_song_dimension_table,
     load_time_dimension_table,
     load_artist_dimension_table] >> run_quality_checks

    run_quality_checks >> end_operator


final_project_dag = final_project()
