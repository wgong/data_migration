#!/usr/bin/env python
# -*- coding: UTF-8-*-

"""
this program supports data migration coding challenge,
 which loads json data into 2 PostgreSql tables: (orders,line_items)
"""

import getopt, sys
import os.path
import time

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
    total_lines = process_data(file_in, file_out, file_param)

    ts2 = time.clock()
    print("Processed %d lines in %.3f sec" % (total_lines, (ts2-ts1) ))

    # exit
    sys.exit(0)
  
if __name__ == "__main__":
    main()