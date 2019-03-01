#!/usr/bin/env python
# -*- coding: UTF-8-*-
import getopt, sys
import os
import os.path
import subprocess
import glob
import time
import json
import psycopg2
from psycopg2.extras import execute_values

def select_data(connection,tbl_name):
    """
    helper function to query data
    """
    cur = connection.cursor()
    sql_1_select = f"""
        SELECT * FROM \"{tbl_name}\" LIMIT 100;
    """ 
    cur.execute(sql_1_select)
    rows = cur.fetchall()
    cur.close()
    return rows

def cleanup_tables(connection,tbl_name):
    """
    helper function to truncate data
    """
    cur = connection.cursor()
    # cleanup
    sql_1_delete = f"""
        truncate table \"{tbl_name}\";
    """ 
    cur.execute(sql_1_delete)
    connection.commit()
    cur.close()

def build_insert_sql(connection, tbl_name, schema_name="public"):
    """
    helper function to construct insert SQL stmt
    """
    cur = connection.cursor()
    # get column info
    sql_1_coldef = f"""
        SELECT sc.table_name, sc.column_name, sc.data_type
        FROM information_schema.columns sc
        WHERE table_schema = '{schema_name}'
        AND table_name = '{tbl_name}'
        order by table_name,ordinal_position;
    """

    cur.execute(sql_1_coldef)
    col_1_defs = cur.fetchall()
    cur.close()

    # print("col_1_defs=\n\t", col_1_defs)
    col_1_list = [(col[1],col[2]) for col in col_1_defs]

    # build column list
    col_1_list_str = ",".join([f"\"{c[0]}\""  for c in col_1_list])

    sql_1_insert = f"""
        INSERT INTO \"{tbl_name}\" ({col_1_list_str})
        VALUES %s;
    """ 
    return sql_1_insert, col_1_list

def process_json_file(connection, file_in, mapTable2JsonObj, batch_size=200, schema_name="public"):
    """
    this function processes one json data file
    """

    try:
        with open(file_in) as f:
            s = f.read()
            try:        
                dic = json.loads(s)
            except:
                print(f"Failed to load json data, abort!")
                raise
    except:
        print(f"Failed to read data from file {file_in}, abort!")
        raise


    tbl_1_name = 'dm_orders'
    tbl_2_name = 'dm_line_items'

    sql_1_insert,col_1_list = build_insert_sql(connection, tbl_1_name, schema_name)
    sql_2_insert,col_2_list = build_insert_sql(connection, tbl_2_name, schema_name)

    # print("sql_1_insert=\n\t", sql_1_insert)
    # print("col_1_list=\n\t", col_1_list)

    cur = connection.cursor()


    # process one json file
    orders = dic[mapTable2JsonObj['dm_orders']]
    nrows_orders = len(orders)
    nrows_line_items = 0
    values_orders, values_line_items = [], []

    # process orders
    for i_orders in range(nrows_orders):
        
        order = orders[i_orders]
        order_id = order['id']
        val_1_list = []
        for c in col_1_list:
            val_1_list.append(order[c[0]])

        values_orders.append(tuple(val_1_list))

        # process line_items
        order_line_items = order[mapTable2JsonObj['dm_line_items']]
        num_order_line_items = len(order_line_items)
        nrows_line_items += num_order_line_items

        i_line_items = 0
        for i_line_items in range(num_order_line_items):
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
        if i_orders > 0 and i_orders % batch_size == 0:
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

    cur.close()
    return (nrows_orders, nrows_line_items)

def load_json_data(file_in, file_param):
    """
    this function supports data migration coding challenge,
    which loads json data into 2 PostgreSql tables: (dm_orders,dm_line_items)
    """

    FLAG_CLEANUP = False  # delete rows for next load
    FLAG_VERIFY = False  # read back for verification

    FLAG_PG_LOCAL = True

    if FLAG_PG_LOCAL:
        db_host = os.environ.get('LOCAL_PG_DB_HOST')
        db_name = os.environ.get('LOCAL_PG_DB_NAME')
        db_user = os.environ.get('LOCAL_PG_DB_USER')
        password = os.environ.get('LOCAL_PG_DB_PASS')
    else:
        db_host = os.environ.get('AWS_PG_DB_HOST')
        db_name = os.environ.get('AWS_PG_DB_NAME')
        db_user = os.environ.get('AWS_PG_DB_USER')
        password = os.environ.get('AWS_PG_DB_PASS')

    schema_name = 'public'

    # table to Json obj
    tbl_1_name = 'dm_orders'
    tbl_2_name = 'dm_line_items'
    mapTable2JsonObj = {tbl_1_name : 'orders', tbl_2_name : 'line_items'}

    ntotal_orders, ntotal_line_items = 0, 0

    # connect to PostgreSQL
    db_connection_string = f"dbname='{db_name}' user='{db_user}' host='{db_host}' password='{password}'"
    try:
        connection = psycopg2.connect(db_connection_string)
    except:
        print(f"Failed to connect to DB: {db_connection_string}, abort!")
        raise

    try:
        with open(file_param) as f:
            BATCH_SIZE = int(f.read())
    except:
        print(f"Failed to read BATCH_SIZE from file {file_param}, use default=200")
        BATCH_SIZE = 200     # size for batch insert

    file_ext = file_in.split('.')[-1].lower()
    if file_ext == 'json':
        file_list = [file_in]
    elif file_ext == 'zip':
        dir_path = os.path.dirname(os.path.abspath(file_in))
        tmp_path = os.path.join(dir_path, "tmp")
        if os.path.exists(tmp_path):
            cmd = f"rm -rf {tmp_path}"
            cmd_out=subprocess.run(cmd.split(), stdout=subprocess.PIPE).stdout.decode('utf-8')
        cmd = f"unzip {file_in} -d {tmp_path}"
        cmd_out=subprocess.run(cmd.split(), stdout=subprocess.PIPE).stdout.decode('utf-8')
        file_list = glob.glob(os.path.join(tmp_path, "*.json"))
    else:
        print(f"Invalid data file type: {file_in}, abort!")
        return (-1,-1)

    for filename_in in file_list:
        print(f"Processing {filename_in} ...")
        nrows_orders, nrows_line_items = \
            process_json_file(connection, filename_in, mapTable2JsonObj, batch_size=BATCH_SIZE, schema_name=schema_name)
        print(f"nrows_orders={nrows_orders}, nrows_line_items={nrows_line_items}")

        ntotal_orders     += nrows_orders
        ntotal_line_items += nrows_line_items

    if FLAG_VERIFY:
        print(select_data(connection,tbl_1_name))
        print(select_data(connection,tbl_2_name))

    if FLAG_CLEANUP:
        cleanup_tables(connection,tbl_1_name)
        cleanup_tables(connection,tbl_2_name)

    # done with DB
    connection.close()

    return (ntotal_orders, ntotal_line_items)

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
    ntotal_orders, ntotal_line_items = load_json_data(file_in, file_param)
    if ntotal_orders == -1 and ntotal_line_items == -1:
        sys.exit(2)

    ts2 = time.clock()
    print(f"Processed {ntotal_orders} orders / {ntotal_line_items} line_items in {(ts2-ts1):.3f} sec")

    # exit
    sys.exit(0)
  
if __name__ == "__main__":
    main()