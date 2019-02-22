#!/usr/bin/env python
# -*- coding: UTF-8-*-

import getopt, sys
import os.path
import codecs
import time
from datetime import datetime


header_map = {}
headers = []

# parse each log line
def parse_line(l, delimitor=','):
    global headers, header_map
    lst = l.split(delimitor)
    if len(lst) != len(headers):
        return None
    t = datetime.strptime(f"{lst[header_map['date']]}T{lst[header_map['time']]}", '%Y-%m-%dT%H:%M:%S')
    t_sec = int(time.mktime(t.timetuple()))
    return [lst[header_map['ip']], t_sec, lst[header_map['date']], lst[header_map['time']] ]

# write each ip session
def write_output(f, ip, lst):   
    date_time_first_req = f"{lst[0][1]} {lst[0][2]}"
    date_time_last_req = f"{lst[-1][1]} {lst[-1][2]}"
    duration = lst[-1][0] - lst[0][0] + 1
    req_count = len(lst)
    f.write(f"{ip},{date_time_first_req},{date_time_last_req},{duration},{req_count}\n")


def process_data(file_in, file_out, file_inactivity):
    """
    Function: 
        process log data
    Inputs: 
        file_in - input weblog filename
        file_out - output filename to store analysis results
        file_inactivity - file with INACTIVITY_PERIOD parameter value
    Outputs: 
        analysis results are written to file_out
        log lines failed parsing are stored in file_err (similar to file_out with .err extension)
    Return: 
        number of lines processed
    """
    global headers, header_map

    with open(file_inactivity) as f:
        INACTIVITY_PERIOD = int(f.read())

    head, tail = os.path.split(file_out)
    # write out bad log line
    file_err = os.path.join(head, tail.split('.')[0] + ".err")

    delimitor=','
    num_lines = 0

    # map to track requests 
    # ip as key, value is a list of reqs within active session
    ip_req_map = {}  

    f_out = open(file_out, 'w')
    f_err = open(file_err, 'w')

    with open(file_in,'r') as f:
        for line in iter(f.readline,''):
            line=line.strip()

            if num_lines < 1:
                # first line is header
                headers=line.split(delimitor)
                ih = 0
                for h in headers:
                    header_map[h.strip()] = ih
                    ih += 1

            else:
                # skip blank / comment line
                if len(line) < 1 or line[0] == '#':
                    continue

                log_data = parse_line(line,delimitor)

                if log_data is None: 
                    f_err.write(f"[ERROR] invalid log: {line}\n")
                    continue

                new_ip, new_ts = log_data[0], log_data[1]

                expired_ips = []
                for ip in ip_req_map:
                    last_ts = ip_req_map[ip][-1][0]
                    session_expired = ((new_ts - last_ts) > INACTIVITY_PERIOD)
                    if session_expired:
                        # write out result
                        write_output(f_out, ip, ip_req_map[ip])

                    if ip == new_ip:
                        # check if this req within last session
                        if session_expired:
                            ip_req_map[ip] = [log_data[1:]]
                        else:
                            ip_req_map[ip].append(log_data[1:])
                    else:
                        if session_expired:
                            # mark for removal
                            expired_ips.append(ip)

                # add new ip
                if not new_ip in ip_req_map:
                    ip_req_map[new_ip] = [log_data[1:]]

                # cleanup expired session
                for ip in expired_ips:
                    del ip_req_map[ip]

            num_lines += 1

    # write remaining ip when reaching end of file
    for ip in ip_req_map:
        write_output(f_out, ip, ip_req_map[ip])
        
    f_err.close()
    f_out.close()
    
    return num_lines

def usage():
    print("")
    print("Usage:")
    print("  python " + sys.argv[0] + ' -i <input.txt> -o <output.txt>')
    print("")
    sys.exit(1)

    
def main():
    # parse param
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hi:o:p:", ["help", "input=","output=","param="])
    except getopt.GetoptError as err:
        print("[%s] %s" %(sys.argv[0],str(err))) 
        usage()
        
    file_in = ""
    file_out = ""
    
    for o, a in opts:
        if o in ("-h", "--help"):
            usage()
        elif o in ("-i", "--input"):
            file_in = a
        elif o in ("-o", "--output"):
            file_out = a
        elif o in ("-p", "--param"):
            file_param = a
        else:
            assert False, "unknown option"
            
    if file_in == "" or not os.path.exists(file_in):
        print("[%s] Invalid input file!" % (sys.argv[0],))
        sys.exit(1)
    else:
        # test read
        try:       
            f = codecs.open(file_in,mode='r',encoding=sys.getfilesystemencoding())
        except IOError:
            print("[%s] Unable to read input file!" % (sys.argv[0],))
            usage()
        else:
            f.close()
    

    if file_out == "":
        print("[%s] Missing output file!" % (sys.argv[0],))
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