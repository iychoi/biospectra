#! /usr/bin/env python

import os
import os.path
import sys

def _cut(cnt, fromPath, toPath):
    count=0

    if not os.path.exists(toPath):
        os.makedirs(toPath)

    head, tail = os.path.split(fromPath)

    newPath = os.path.join(toPath, tail)
    if os.path.abspath(newPath) == os.path.abspath(fromPath):
        newPath = newPath + ".new"

    print "copying first " + str(cnt) + " reads from " + fromPath + " to " + newPath

    fn = open(newPath, "w")
    f = open(fromPath)
    for line in f:
        if(line.startswith(">")):
            count = count + 1

        if(count>cnt):
            break
        
        fn.write(line)
    f.close()
    fn.close()

    count = count - 1

    if count != cnt:
        print "total reads in " + fromPath + " is less than required (" + str(count) + " - copied anyway"

def cut(cnt, fromPath, toPath):
    if os.path.isdir(fromPath):
        for p in fromPath:
            _cut(cnt, p, toPath)
    else:
        _cut(cnt, fromPath, toPath)

def main():
    if len(sys.argv) != 4:
        print "arguments are not given correctly"
        print "python cut_reads.py <read_len> <in_path> <out_path>"
        return

    cut(int(sys.argv[1]), sys.argv[2], sys.argv[3])

if __name__ == "__main__":
    main()
