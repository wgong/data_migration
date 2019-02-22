#!/usr/bin/env python
# -*- coding: UTF-8-*-


import getopt, sys
import os.path
import time
import os
import json
import psycopg2
from psycopg2.extras import execute_values

def load_json_data(file_in, file_param):
    """
    this function supports data migration coding challenge,
    which loads json data into 2 PostgreSql tables: (dm_orders,dm_line_items)
    """


    FLAG_CLEANUP = False  # delete rows for next load
    FLAG_VERIFY = False  # read back for verification

    # config params
    # file_in = '2017-11-10.json'
    # file_in = 'test1.json'

    db_host = os.environ.get('AWS_PG_DB_HOST')
    db_name = os.environ.get('AWS_PG_DB_NAME')
    db_user = os.environ.get('AWS_PG_DB_USER')
    password = os.environ.get('AWS_PG_DB_PASS')
    schema_name = 'public'

    # table to Json obj
    mapTable2JsonObj = {'dm_orders':'orders', 'dm_line_items':'line_items'}

    try:
        with open(file_param) as f:
            BATCH_SIZE = int(f.read())
    except:
        print(f"Failed to read BATCH_SIZE from file {file_param}, use default=200")
        BATCH_SIZE = 200     # size for batch insert

    try:
        with open(file_in) as f:
            s = f.read()
    except:
        print(f"Failed to read data from file {file_in}, abort!")
        return (-1,)

    try:        
        dic = json.loads(s)
    except:
        print(f"Failed to load json data, abort!")
        return (-2,)

    # connect to PostgreSQL
    db_connection_string = f"dbname='{db_name}' user='{db_user}' host='{db_host}' password='{password}'"
    try:
        connection = psycopg2.connect(db_connection_string)
    except:
        print(f"Failed to connect to DB: {db_connection_string}, abort!")
        return (-3,)
        
    cur = connection.cursor()

    tbl_1_name = 'dm_orders'
    tbl_2_name = 'dm_line_items'

    # get column info
    sql_1_coldef = f"""
        SELECT sc.table_name, sc.column_name, sc.data_type
        FROM information_schema.columns sc
        WHERE table_schema = '{schema_name}'
        AND table_name = '{tbl_1_name}'
        order by table_name,ordinal_position;
    """

    cur.execute(sql_1_coldef)
    col_1_defs = cur.fetchall()
    col_1_list = [(col[1],col[2]) for col in col_1_defs]

    # get column info
    sql_2_coldef = f"""
        SELECT sc.table_name, sc.column_name, sc.data_type
        FROM information_schema.columns sc
        WHERE table_schema = '{schema_name}'
        AND table_name = '{tbl_2_name}'
        order by table_name,ordinal_position;
    """

    cur.execute(sql_2_coldef)
    col_2_defs = cur.fetchall()
    col_2_list = [(col[1],col[2]) for col in col_2_defs]


    # build SQL insert
    col_1_list_str = ",".join([f"\"{c[0]}\""  for c in col_1_list])
    col_2_list_str = ",".join([f"\"{c[0]}\""  for c in col_2_list])

    sql_1_insert = f"""
        INSERT INTO \"{tbl_1_name}\" ({col_1_list_str})
        VALUES %s;
    """ 

    sql_2_insert = f"""
        INSERT INTO \"{tbl_2_name}\" ({col_2_list_str})
        VALUES %s;
    """ 

    orders = dic[mapTable2JsonObj['dm_orders']]

    nrows_line_items = 0

    values_orders, values_line_items = [], []
    # process orders
    for i_orders in range(len(orders)):
        
        order = orders[i_orders]
        order_id = order['id']
        val_1_list = []
        for c in col_1_list:
            val_1_list.append(order[c[0]])

        values_orders.append(tuple(val_1_list))

        # process line_items
        order_line_items = order[mapTable2JsonObj['dm_line_items']]

        nrows_line_items += len(order_line_items)

        i_line_items = 0
        for i_line_items in range(len(order_line_items)):
            val_2_list = []
            for c in col_2_list:
                col_name, col_type = c[0], c[1]
                if col_name == 'order_id':
                    val = order_id
                else:
                    val = order_line_items[i_line_items][col_name]

                val_2_list.append(val)
            
            values_line_items.append(tuple(val_2_list))

        # write to DB
        if i_orders > 0 and i_orders % BATCH_SIZE == 0:
            execute_values(cur, sql_1_insert, values_orders)
            execute_values(cur, sql_2_insert, values_line_items)
            connection.commit()  # write to db
            values_orders, values_line_items = [], []

    # final flush
    if values_orders:
        execute_values(cur, sql_1_insert, values_orders)
        connection.commit()  # write to db
    if values_line_items:
        execute_values(cur, sql_2_insert, values_line_items)
        connection.commit()  # write to db


    if FLAG_VERIFY:
        # build SQL select
        col_list_str = ",".join([f"\"{c[0]}\""  for c in col_1_list])
        sql_1_select = f"""
            SELECT {col_list_str} FROM \"{tbl_1_name}\";
        """ 
        sql_1_select

        cur.execute(sql_1_select)
        rows = cur.fetchall()

        # build SQL select
        col_list_str = ",".join([f"\"{c[0]}\""  for c in col_2_list])
        sql_2_select = f"""
            SELECT {col_list_str} FROM \"{tbl_2_name}\";
        """ 

        cur.execute(sql_2_select)
        rows = cur.fetchall()

    if FLAG_CLEANUP:
        # cleanup
        sql_1_delete = f"""
            DELETE FROM \"{tbl_1_name}\";
        """ 
        cur.execute(sql_1_delete)
        connection.commit()

        sql_2_delete = f"""
            DELETE FROM \"{tbl_2_name}\";
        """ 
        cur.execute(sql_2_delete)
        connection.commit()

    # done with DB
    connection.close()

    return (len(orders), nrows_line_items)

def usage():
    print("")
    print("Usage:")
    print("  python " + sys.argv[0] + ' -i <input.txt> -p batch_size.config')
    print("")
    sys.exit(1)

def main():
    # parse param
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hi:p:", ["help", "input=","param="])
    except getopt.GetoptError as err:
        print("[%s] %s" %(sys.argv[0],str(err))) 
        usage()
        
    file_in = ""
    
    for o, a in opts:
        if o in ("-h", "--help"):
            usage()
        elif o in ("-i", "--input"):
            file_in = a
        elif o in ("-p", "--param"):
            file_param = a
        else:
            assert False, "unknown option"
            
    if file_in == "" or not os.path.exists(file_in):
        print("[%s] Invalid input file!" % (sys.argv[0],))
        sys.exit(1)

    if file_param == "" or not os.path.exists(file_param):
        print("[%s] Invalid param file!" % (sys.argv[0],))
        sys.exit(1)

    ts1 = time.clock()

    # start processing
    total_rows = load_json_data(file_in, file_param)

    ts2 = time.clock()
    print(f"Processed {total_rows} rows in {(ts2-ts1):.3f} sec")

    # exit
    sys.exit(0)
  
if __name__ == "__main__":
    main()