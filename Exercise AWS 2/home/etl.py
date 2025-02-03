import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """ This function loads the data into stg_events and stg_songs tables from the S3 bucket which has the song and log data.
    Input parameters are cur (cursor name) and conn (connection name) """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """ This function insert data into fact and dimensional tables from the staging tables stg_events and stg_songs.
    Input parameters are cur (cursor name) and conn (connection name) """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """ This is the main function where the code begins and control flows"""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
