from datetime import datetime, timedelta
import pendulum
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common.final_project_sql_statements import SqlQueries
from airflow.operators.postgres_operator import PostgresOperator

s3_bucket = 'sparkify-stream'
song_s3_key = "song-data/"
log_s3_key = "log-data/"
log_json_file = "log_json_path.json"

default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    'depends_on_past':False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup': False
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    create_staging_events_table_task=PostgresOperator(
       task_id="create_table_staging_events",
        postgres_conn_id="redshift",
        sql=SqlQueries.CREATE_staging_events_TABLE_SQL
    )

    create_staging_songs_table_task=PostgresOperator(
       task_id="create_table_staging_songs",
        postgres_conn_id="redshift",
        sql=SqlQueries.CREATE_staging_songs_TABLE_SQL
    )

    create_songplays_table_task=PostgresOperator(
       task_id="create_table_songplays",
        postgres_conn_id="redshift",
        sql=SqlQueries.CREATE_songplays_TABLE_SQL
    )

    create_users_table_task=PostgresOperator(
       task_id="create_table_users",
        postgres_conn_id="redshift",
        sql=SqlQueries.CREATE_users_TABLE_SQL
    )


    create_songs_table_task=PostgresOperator(
       task_id="create_table_songs",
        postgres_conn_id="redshift",
        sql=SqlQueries.CREATE_songs_TABLE_SQL
    )

    create_artists_table_task=PostgresOperator(
       task_id="create_table_artists",
        postgres_conn_id="redshift",
        sql=SqlQueries.CREATE_artists_TABLE_SQL
    )

    create_time_table_task=PostgresOperator(
       task_id="create_table_time",
        postgres_conn_id="redshift",
        sql=SqlQueries.CREATE_time_TABLE_SQL
    )

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        table="staging_events",
        s3_bucket=s3_bucket,
        s3_key=log_s3_key,
        file_format="JSON",
        log_json_file=log_json_file,
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials"
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        table="staging_songs",
        s3_bucket=s3_bucket,
        s3_key=song_s3_key,
        file_format="JSON",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials"
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id='redshift',
        sql_statement=SqlQueries.songplay_table_insert,
        table="songplays",
        append_mode=True
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id="redshift",
        sql_statement=SqlQueries.user_table_insert,
        append_mode="False",
        table="users"
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id="Load_song_dim_table",
        redshift_conn_id="redshift",
        sql_statement=SqlQueries.song_table_insert,
        append_mode="False",
        table="songs"
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id="redshift",
        sql_statement=SqlQueries.artist_table_insert,
        append_mode="False",
        table="artists"
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id="redshift",
        sql_statement=SqlQueries.time_table_insert,
        append_mode="False",
        table="time"
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id="redshift",
        tables=["artists", "songplays", "songs", "time", "users"]
    )

    # Define task dependencies
    start_operator >> [create_staging_events_table_task, create_staging_songs_table_task]
    create_staging_events_table_task >> stage_events_to_redshift
    create_staging_songs_table_task >> stage_songs_to_redshift
    
    stage_events_to_redshift >> create_songplays_table_task
    stage_songs_to_redshift >> create_songplays_table_task

    create_songplays_table_task >> load_songplays_table

    load_songplays_table >> [create_users_table_task,create_songs_table_task,create_artists_table_task,create_time_table_task]

    create_users_table_task >> load_user_dimension_table
    create_songs_table_task >> load_song_dimension_table
    create_artists_table_task >> load_artist_dimension_table
    create_time_table_task >> load_time_dimension_table

    [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks

final_project_dag = final_project()