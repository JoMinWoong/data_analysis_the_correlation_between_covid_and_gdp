import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    for query in drop_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()

        
def create_tables(cur, conn):
    for query in create_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('app.cfg')
    
    RED_HOST = config.get('REDSHIFT', 'HOST')
    RED_DBNAME = config.get('REDSHIFT', 'DBNAME')
    RED_USER = config.get('REDSHIFT', 'USER')
    RED_PASSWORD = config.get('REDSHIFT', 'PASSWORD')
    RED_PORT = config.get('REDSHIFT', 'PORT')

    conn=psycopg2.connect(dbname= RED_DBNAME, host=RED_HOST, port= RED_PORT, user= RED_USER, password= RED_PASSWORD)
    cur = conn.cursor()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)
    
    conn.close()


if __name__ == "__main__":
  main()    