#! /usr/bin/env python

import os
import os.path
import sys

def _cut(path, cnt):
    print "cutting first 500 reads", path
    
    count=0
    fn = open(path + ".new", "w")
    f = open(path)
    for line in f:
        if(line.startswith(">")):
            count=count+1;

        if(count>cnt):
            break;
        
        fn.write(line)
    f.close()
    fn.close()

def cut(path, cnt):
    for p in path:
        _cut(p, cnt);

def main():
    cut(sys.argv[2:], int(sys.argv[1]))

if __name__ == "__main__":
    main()
