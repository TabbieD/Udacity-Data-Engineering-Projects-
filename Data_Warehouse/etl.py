import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print(e)
    print("STAGING TABLES LOADED!!!")


def insert_tables(cur, conn):
    for query in insert_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print(e)
    print("ETL COMPLETE!!!")


def main():
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))
    DWH_DB_USER = config.get('DWH', 'DWH_DB_USER')
    DWH_DB_PASSWORD = config.get('DWH', 'DWH_DB_PASSWORD')
    DWH_ENDPOINT = config.get('CLUSTER', 'dwh_endpoint')
    DWH_PORT = config.get('DWH', 'DWH_PORT')
    DWH_DB = config.get('DWH', 'DWH_DB')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}"
                            .format(DWH_ENDPOINT, DWH_DB,
                                    DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
