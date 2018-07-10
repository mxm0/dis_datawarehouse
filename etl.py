#!/usr/bin/python3
import psycopg2
import psycopg2.extras
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
    query = 'SELECT landid.name, regionid.name, stadtid.name, shopid.name FROM landid INNER JOIN regionid ON landid.landid = regionid.landid INNER JOIN stadtid ON stadtid.stadtid = regionid.regionid INNER JOIN shopid ON shopid.stadtid = stadtid.stadtid'
    cursor.execute(query)
    Locations = {}
    for row in cursor:
        Locations[row[3]] = row[:3]

    query = 'SELECT productcategoryid.name, productfamilyid.name, productgroupid.name, articleid.name, preis FROM productcategoryid INNER JOIN productfamilyid ON productcategoryid.productcategoryid = productfamilyid.productcategoryid INNER JOIN productgroupid ON productfamilyid.productfamilyid = productgroupid.productfamilyid INNER JOIN articleid ON articleid.productgroupid = productgroupid.productgroupid'
    cursor.execute(query)
    Products = {}
    for row in cursor:
        Products[row[3]] = row[:3] + (row[4],)
    
    return Products, Locations

def get_quarter(month):
    if month <= 3:
        return 1
    elif month > 3 and month <=6:
        return 2
    elif month > 6 and month <=9:
        return 3
    else:
        return 4

def get_foreign_keys(cursor):
    query_p = 'SELECT productid FROM productdim'
    query_l = 'SELECT locationid FROM locationdim'
    query_t = 'SELECT timeid FROM timedim'
    
    # Get foreign keys
    cursor.execute(query_p)
    product_cur = cursor
    product = product_cur.fetchall()
    
    cursor.execute(query_l)
    location_cur = cursor
    location = location_cur.fetchall()
    
    cursor.execute(query_t)
    time_cur = cursor
    time = time_cur.fetchall()
    
    foreign_keys = []
 
    # Build tuple with foreign keys from all dimensions
    for p_key, l_key, t_key in zip(product, location, time):
        foreign_keys.append(p_key + l_key + t_key)
   
    return foreign_keys 

if __name__ == '__main__':
    # Connect to database and get cursor
    conn = connect()
    cur = conn.cursor()
    
    # Extract data from database
    Products, Locations = extract(cur)
    
    # Extract data from CSV
    with open('data/sales.csv', newline='', encoding = 'iso8859_2') as SalesFile:
        sales = csv.reader(SalesFile, delimiter = ';', quotechar='|')
        # Skip columns name
        next(sales)
        
        args_time = []
        args_location = []
        args_product = []
        args_sales = []
 
        for i, sale in enumerate(sales):
            # Set args for location dimension, product dimension and sales
            
            try:  
                # Check if profit or sales are 0
                profit = float(sale[4].replace(',', '.'))
                if len(sale) < 5 or int(sale[3]) == 0 or profit == 0:
                    continue

                # Sales
                args_sales.append((int(sale[3]), profit))

                # Location
                land, region, state = Locations[sale[1]]
                args_location.append((land, region, state, sale[1])) 
                
                # Product
                category, family, group, price = Products[sale[2]]
                args_product.append((category, family, group, price, sale[2])) 
                
                # Set args for time dimension
                day, month, year = sale[0].split('.')
                quarter = get_quarter(int(month))
                args_time.append((int(day), int(month), int(year), quarter))

            except KeyError:
                print("Article or Store not defined")
            except ValueError:
                print("Price or Profit not defined")
        
    # Build queries structure 
    query_time     = 'INSERT INTO TimeDim (day, month, year, quarter) VALUES %s'
    query_location = 'INSERT INTO LocationDim (land, region, state, shop) VALUES %s'
    query_product  = 'INSERT INTO ProductDim (category, family, class, price, article) VALUES %s'
    query_facts    = 'INSERT INTO SalesFact (productid, locationid, timeid, salesunit, profit) VALUES %s'
    psycopg2.extras.execute_values(cur, query_time, args_time, template=None, page_size = 100)
    psycopg2.extras.execute_values(cur, query_product, args_product, template=None, page_size = 100)
    psycopg2.extras.execute_values(cur, query_location, args_location, template=None, page_size = 100)

    foreign_keys = get_foreign_keys(cur)
    args_facts = []
    for keys, sales in zip(foreign_keys, args_sales):
        args_facts.append(keys + sales)
    
    psycopg2.extras.execute_values(cur, query_facts, args_facts, template=None, page_size = 100)
    # Close the communication with PostgreSQL
    conn.commit()
    cur.close()
