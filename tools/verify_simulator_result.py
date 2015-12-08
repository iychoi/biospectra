#! /usr/bin/env python

import os
import os.path
import sys
import json

success = 0

def _extractGenome(header):
    headers = header.split('|')
    genome = headers[4].strip()
    pos = genome.find(",")        
    if pos > 0:
        genome = genome[:pos]

    pos = genome.find(" chromosome")
    if pos > 0:
        genome = genome[:pos]
    return genome

def _parseClassified(path):
    f = open(path)
    for line in f:
        j = json.loads(line)

        query = j['query']
        query_header = j['query_header']
        result = j['result']
        ctype = j['type']

        if ctype == "CLASSIFIED":
            r = result[0]
            #print "> " + ctype + "\t" + r['header'] + "\t" + str(r['score'])
            genome = _extractGenome(r['header'])
            source = _extractGenome(query_header)
            if genome == source:
                global success
                success = success + 1

    f.close()

def _parseVague(path):
    count=0
    f = open(path)
    for line in f:
        j = json.loads(line)

        query = j['query']
        query_header = j['query_header']
        result = j['result']
        ctype = j['type']

        """
        if ctype == "VAGUE":
            print "> "
            for r in result:
                print ctype + "\t" + r['header'] + "\t" + str(r['score'])
        """

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

        """
        if ctype == "UNKNOWN":
            print "> " + ctype + query_header
        """

    f.close()

def _parse(path):
    print "parsing", path
    _parseClassified(path);
    _parseVague(path);
    _parseUnknown(path);

def parse(path):
    if os.path.isdir(path):
        for p in path:
            _parse(p)
    else:
        _parse(path)

    print "total successful classification =", success

def main():
    parse(sys.argv[1])

if __name__ == "__main__":
    main()
