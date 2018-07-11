#!/usr/bin/python3
import psycopg2
import psycopg2.extras
import csv
from connect import connect
from collections import OrderedDict

def print_crosstable():
    conn = connect()
    cur = conn.cursor()
    
    query = 'SELECT locationdim.state, timedim.quarter, timedim.year, sum(salesfact.salesunit)\
             FROM salesfact\
             INNER JOIN productdim ON salesfact.productid = productdim.productid\
             INNER JOIN locationdim ON salesfact.locationid = locationdim.locationid\
             INNER JOIN timedim ON salesfact.timeid = timedim.timeid\
             GROUP BY GROUPING SETS ((locationdim.state, timedim.year, timedim.quarter))\
             ORDER BY locationdim.state, timedim.year, timedim.quarter'

    cur.execute(query)
    rows = cur.fetchall()
    data_dict = {}
    for row in rows:
        data_dict[(row[0], row[1], row[2])] = [row[3]] 
    data_dict = OrderedDict(sorted(data_dict.items()))
    
    # Get products names
    query = 'SELECT article FROM productdim GROUP BY article ORDER BY article'

    cur.execute(query)
    rows = cur.fetchall()
    products = []
    for i, article in enumerate(rows[:10]):
        products.append('p' + str(i))
    
    # Get product sales for city, quarter and year
    query = 'SELECT locationdim.state, timedim.quarter, timedim.year, sum(salesfact.salesunit), productdim.article\
             FROM salesfact\
             INNER JOIN productdim ON salesfact.productid = productdim.productid\
             INNER JOIN locationdim ON salesfact.locationid = locationdim.locationid\
             INNER JOIN timedim ON salesfact.timeid = timedim.timeid\
             GROUP BY GROUPING SETS ((locationdim.state, timedim.year, timedim.quarter, productdim.article))\
             ORDER BY productdim.article'

    cur.execute(query)
    rows = cur.fetchall()
    for row in rows:
        data_dict[(row[0], row[1], row[2])].append(row[3])  
    template = "{:<28}"

    for p in products:
        template += " | {:<6}" 
    template += " | {:<10}"
    print(template.format('Sales', *products, 'Total\n'))
 
    # Get intra totals
    query = 'SELECT locationdim.state, sum(salesfact.salesunit), productdim.article\
             FROM salesfact\
             INNER JOIN productdim ON salesfact.productid = productdim.productid\
             INNER JOIN locationdim ON salesfact.locationid = locationdim.locationid\
             INNER JOIN timedim ON salesfact.timeid = timedim.timeid\
             GROUP BY GROUPING SETS ((locationdim.state, productdim.article))\
             ORDER BY locationdim.state, productdim.article' 

    cur.execute(query)
    p_totals = cur.fetchall()
    intra_totals = {}
    for p_total in p_totals:
        intra_totals[p_total[0]] = []
    
    for p_total in p_totals:
        intra_totals[p_total[0]].append(p_total[1])
    print(intra_totals['Berlin']) 
    # Print sales values   
    prev_state = ""
    start = True
    for k, v in data_dict.items():
        if prev_state != k[0] and not start:
           print(template.format('Total', *intra_totals[prev_state][:10], sum(intra_totals[prev_state]))) 
           print('-' * 130)
        prev_state = k[0]
        total = v[0]
        sold  = v[1:11] 
        key = k[0] + ', Quarter ' + str(k[1]) + ', ' + str(k[2])
        print(template.format(key, *sold, total))
        start = False
    print(template.format('Total', *intra_totals[prev_state][:10], sum(intra_totals[prev_state])))
    print('-' * 130)

    # Get whole table totals
    query = 'SELECT sum(salesfact.salesunit), productdim.article\
             FROM salesfact\
             INNER JOIN productdim ON salesfact.productid = productdim.productid\
             INNER JOIN locationdim ON salesfact.locationid = locationdim.locationid\
             INNER JOIN timedim ON salesfact.timeid = timedim.timeid\
             GROUP BY GROUPING SETS (productdim.article)\
             ORDER BY productdim.article' 
    cur.execute(query)
    rows = cur.fetchall()
    table_totals = []
    for totals in rows[:10]:
        table_totals.append(totals[0])

    print(template.format('Table Total', *table_totals, sum(table_totals)))


if __name__ == '__main__':
   print_crosstable() 
