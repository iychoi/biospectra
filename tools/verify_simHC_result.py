#! /usr/bin/env python

import os
import os.path
import sys
import json

def _parseClassified(path):
    res_dict = {}
    f = open(path)
    for line in f:
        j = json.loads(line)

        query = j['query']
        query_header = j['query_header']
        result = j['result']
        ctype = j['type']
        taxon_rank = j['taxon_rank']
        taxon_name = j['taxon_name']

        if ctype == "CLASSIFIED":
            genome = taxon_name + "(" + taxon_rank + ")"
            if genome in res_dict:
                res_dict[genome] = res_dict[genome] + 1
            else:
                res_dict[genome] = 1

    f.close()

    for k in res_dict:
        print k + "\t" + str(res_dict[k])

def _parseVague(path):
    count=0
    f = open(path)
    for line in f:
        j = json.loads(line)

        query = j['query']
        query_header = j['query_header']
        result = j['result']
        ctype = j['type']

        if ctype == "VAGUE":
            print "> "
            for r in result:
                print ctype + "\t" + r['header'] + "\t" + str(r['score'])

    f.close()

def _parseUnknown(path):
    count=0
    f = open(path)
    for line in f:
        j = json.loads(line)

        query = j['query']
        query_header = j['query_header']
        result = j['result']
        ctype = j['type']

        if ctype == "UNKNOWN":
            print "> " + ctype + query_header

    f.close()

def _parse(path):
    if path.endswith(".result"):
        print "parsing", path
        _parseClassified(path);
        _parseVague(path);
        _parseUnknown(path);
        return True
    return False

def parse(path):
    if os.path.isdir(path):
        for p in os.listdir(path):
            parse(os.path.join(path, p))
    else:
        _parse(path)

def main():
    parse(sys.argv[1])

if __name__ == "__main__":
    main()
