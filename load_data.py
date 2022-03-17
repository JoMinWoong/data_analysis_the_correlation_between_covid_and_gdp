import configparser
import psycopg2
from sql_queries import insert_table_queries


def load_data(cur, conn):
    for query in insert_table_queries:
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
    
    load_data(cur, conn)    
    conn.close()


if __name__ == "__main__":
  main()    