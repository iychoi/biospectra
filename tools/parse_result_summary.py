#! /usr/bin/env python

import os
import os.path
import sys
import json

total_reads=0
total_vague_reads=0
total_classified_reads=0
total_unknown_reads=0
total_search_time=0

def _parse(path):
    print "parsing", path
    count=0
    f = open(path)
    j = json.load(f)

    t = j['total']
    print "total=" + str(t)
    global total_reads
    total_reads += t

    v = j['vague']
    print "vague=" + str(v)
    global total_vague_reads
    total_vague_reads += v

    c = 0
    if 'classified' in j:
        c = j['classified']
    elif 'classifed' in j:
        c = j['classifed']

    print "classified=" + str(c)
    global total_classified_reads 
    total_classified_reads += c
    
    u = j['unknown']
    print "unknown=" + str(u)
    global total_unknown_reads
    total_unknown_reads += u

    et = j['end_time']
    st = j['start_time']
    global total_search_time
    total_search_time += (et-st)/1000

    f.close()

def parse(path):
    for p in path:
        _parse(p);

    print "============================"
    print "grand total=", total_reads
    print "grand total - vague=", total_vague_reads
    print "grand total - classified=", total_classified_reads
    print "grand total - unknown=", total_unknown_reads
    print "total time taken=", total_search_time, "in sec"

def main():
    parse(sys.argv[1:])

if __name__ == "__main__":
    main()
