#! /usr/bin/env python

import os
import os.path
import sys
import sqlite3
import json

conn = None

fasta_extensions = [
    ".fa",
    ".fa.gz",
    ".ffn.gz",
    ".ffn",
    ".fna.gz",
    ".fna",
    ".fasta",
    ".fasta.gz"
]


def conn():
    global conn
    conn = sqlite3.connect('ncbi_taxon.db')

def disconn():
    conn.close()


def readHierarchyByTaxid(taxid):
    query = "select taxid, name, parent, rank from tree where taxid = " + str(taxid)

    cursor = conn.execute(query)
    ret = {}
    for row in cursor:
        ret["taxid"] = row[0]
        ret["name"] = row[1]
        ret["parent"] = row[2]
        ret["rank"] = row[3]
        break

    return ret

def readHierarchyByGenbankid(gid):
    query = "select t.taxid as taxid, t.name as name, t.parent as parent, t.rank as rank from gi_taxid g, tree t where g.taxid = t.taxid and g.gi = " + str(gid)

    cursor = conn.execute(query)
    ret = {}
    for row in cursor:
        ret["taxid"] = row[0]
        ret["name"] = row[1]
        ret["parent"] = row[2]
        ret["rank"] = row[3]
        break

    return ret

def readHierarchyByName(name):
    query = "select taxid, name, parent, rank from tree where name = '" + name + "'"

    cursor = conn.execute(query)
    ret = {}
    for row in cursor:
        ret["taxid"] = row[0]
        ret["name"] = row[1]
        ret["parent"] = row[2]
        ret["rank"] = row[3]
        break

    return ret

def _genHierarchyFile(path):
    print "generating a taxonomy hierarchy file - ", path
    pname = os.path.basename(os.path.dirname(path))
    splits = pname.split("_")
    for x in range(len(splits), 0, -1):
        speciesName = ""
        for y in range(0, x):
            speciesName += splits[y] + " "
       
        speciesName = speciesName.strip()
        ret = readHierarchyByName(speciesName)
        if len(ret) > 0:
            retarr=[]
            retarr.append(ret)
            parent_taxid = ret["parent"]

            while int(parent_taxid) > 0:
                pret = readHierarchyByTaxid(parent_taxid)
                if len(pret) > 0:
                    if pret["taxid"] != pret["parent"]:
                        retarr.append(pret)
                        parent_taxid = pret["parent"]
                    else:
                        retarr.append(pret)
                        break;
                else:
                    break;

            f = open(path + ".taxd", "w")
            f.write(json.dumps(retarr))
            f.close()
            break;

def genHierarchyFile(path):
    if os.path.isdir(path):
        for p in os.listdir(path):
            genHierarchyFile(os.path.join(path, p))
    else:
        for ext in fasta_extensions:
            if path.endswith(ext):
                print "fasta file found - ", path
                _genHierarchyFile(path)
                break;

def main():
    conn()
    if sys.argv[1] == "tid":
        print readHierarchyByTaxid(sys.argv[2])
    elif sys.argv[1] == "gid":
        print readHierarchyByGenbankid(sys.argv[2])
    elif sys.argv[1] == "name":
        print readHierarchyByName(sys.argv[2])
    elif sys.argv[1] == "ncbi":
        genHierarchyFile(sys.argv[2])

    disconn()

if __name__ == "__main__":
    main()
