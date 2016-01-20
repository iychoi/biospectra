#! /usr/bin/env python

import os
import os.path
import sys

def _sumSize(path):
    size = os.path.getsize(path)
    print path, "=", size, "bytes"
    return size

def sumSize(path):
    sizeTotal = 0;
    if os.path.isdir(path):
        for p in os.listdir(path):
            sizeTotal += sumSize(os.path.join(path, p))
    else:
        sizeTotal += _sumSize(path)

    return sizeTotal

def main():
    sizeTotal = sumSize(sys.argv[1])
    print "total size", "=", sizeTotal, "bytes"
    print "total size", "=", sizeTotal/1024, "kilobytes"
    print "total size", "=", sizeTotal/1024/1024, "megabytes"
    print "total size", "=", sizeTotal/1024/1024/1024, "gigabytes"

if __name__ == "__main__":
    main()
