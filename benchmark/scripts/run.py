#! /usr/bin/env python

import os
import sys
import shutil
import subprocess
import json

from datetime import datetime

QUERY_ALGS = [
    "NAIVE_KMER",
    "CHAIN_PROXIMITY",
    "PAIRED_PROXIMITY"
]

def createConfig(k, qalg):
    src = "local_conf.json.template"
    if not os.path.exists(src):
        raise Exception("unable to find configuration template - " + src)

    dst = "local_conf_" + str(k) + "_" + qalg + ".json"
    shutil.copyfile(src, dst)

    subprocess.call("sed -i 's/{size}/" + str(k) + "/g' " + dst, shell=True)
    subprocess.call("sed -i 's/{qalg}/" + qalg + "/g' " + dst, shell=True)
    return os.path.abspath(dst)

def createIndex(k, config):
    print "Creating an index -", k
    cmd = "cd ../../; "
    cmd += "./biospectra_indexing.sh "
    cmd += "-j " + config + " "
    cmd += "-r " + "../mount/bacteria_ncbi_reference"

    start_time = datetime.now()

    subprocess.call(cmd, shell=True)

    end_time = datetime.now()

    duration = end_time - start_time
    print "Duration: {}".format(duration)

def classify(k, qalg, config):
    print "Classify - ", k
    cmd = "cd ../../;"
    cmd += "./biospectra_local_classify.sh "
    cmd += "-j " + config + " "
    cmd += "-in " + "../mount/simHC.20.500 "
    cmd += "-out " + "../mount/simHC.20.500_" + str(k) + "_" + qalg

    start_time = datetime.now()

    subprocess.call(cmd, shell=True)

    end_time = datetime.now()

    duration = end_time - start_time
    print "Duration: {}".format(duration)

def cleanupConfig(config):
    if os.path.exists(config):
        os.remove(config)

def cleanupIndex(config):
    if os.path.exists(config):
        with open(config) as configFile:    
            conf = json.load(configFile)
            indexPath = "../../" + conf["index_path"]
            if os.path.exists(indexPath):
                print "removing -", indexPath
                shutil.rmtree(indexPath)

def go(k):
    create_index = True
    created_config = []
    for qalg in QUERY_ALGS:
        config = createConfig(k, qalg)
	created_config.append(config)
        if create_index:
            createIndex(k, config)
            create_index = False
    
        classify(k, qalg, config)

    """
    Clean Up
    """
    for config in created_config:
        cleanupIndex(config)
        cleanupConfig(config)

def main():
    for k in range(5, 25, 1):
        go(k)

if __name__ == "__main__":
    main()
