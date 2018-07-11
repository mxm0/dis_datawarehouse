#~/usr/bin/python3
from config import config
import psycopg2

def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection paramaters
        params = config()
 
        # connect to the PostgreSQL
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

        return conn
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
          
