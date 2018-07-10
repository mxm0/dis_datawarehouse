#!/usr/bin/python3
import psycopg2
from config import config
import csv
 
def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()
 
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
 
        return conn        
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def extract(cursor):
    cur.execute('SELECT * FROM articleid')
    Locations = cur
    
    cur.execute('SELECT * FROM shopid') 
    Products = cur
     
    return Products, Locations

if __name__ == '__main__':
    # Connect to database and get cursor
    cur = connect().cursor()
    
    # Extract data from database
    Products, Locations =  extract(cur)
    
    # Extract data from CSV
    with open('data/sales.csv', newline='', encoding = 'utf-8') as SalesFile:
        sales = csv.reader(SalesFile, delimiter = ';', quotechar='|')
        for i, sale in enumerate(sales):
            print(', '.join(sale))
            if i > 9:
                break
    # Close the communication with PostgreSQL
    cur.close()

