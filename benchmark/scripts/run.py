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

def runCommandSync(cmd):
    sys.stdout.flush()
    subprocess.call(cmd, shell=True)
    sys.stdout.flush()

def createConfig(k, qalg):
    src = "local_conf.json.template"
    if not os.path.exists(src):
        raise Exception("unable to find configuration template - " + src)

    dst = "local_conf_" + str(k) + "_" + qalg + ".json"
    shutil.copyfile(src, dst)

    runCommandSync("sed -i 's/{size}/" + str(k) + "/g' " + dst)
    runCommandSync("sed -i 's/{qalg}/" + qalg + "/g' " + dst)
    return os.path.abspath(dst)

def createIndex(k, config):
    print "Creating an index -", k
    cmd = "cd ../../; "
    cmd += "./biospectra_indexing.sh "
    cmd += "-j " + config + " "
    cmd += "-r " + "benchmark/bacteria_ncbi_reference"

    start_time = datetime.now()

    runCommandSync(cmd)

    end_time = datetime.now()

    duration = end_time - start_time
    print "Duration: ", duration

def classify(k, qalg, config):
    print "Classify -", k, "with", qalg
    cmd = "cd ../../;"
    cmd += "./biospectra_local_classify.sh "
    cmd += "-j " + config + " "
    cmd += "-in " + "benchmark/simHC.20.500 "
    cmd += "-out " + "benchmark/simHC.20.500_" + str(k) + "_" + qalg

    start_time = datetime.now()

    runCommandSync(cmd)

    end_time = datetime.now()

    duration = end_time - start_time
    print "Duration: ", duration

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

def calcIndexSize(config):
    if os.path.exists(config):
        with open(config) as configFile:
            conf = json.load(configFile)
            indexPath = "../../" + conf["index_path"]
            if os.path.exists(indexPath):
                return sumSize(indexPath)

    return 0

def login(username, password):
    session = smtplib.SMTP('smtp.gmail.com', 587)
    session.ehlo()
    session.starttls()
    session.login(username, password)
    return session

def sendmail(session, username, recipient, subject, body):
    headers = "\r\n".join(["from: " + username,
                           "subject: " + subject,
                           "to: " + recipient,
                           "mime-version: 1.0",
                           "content-type: text/html"])

    # body_of_email can be plaintext or html!           
    content = headers + "\r\n\r\n" + body
    session.sendmail(username, recipient, content)

def trySendEmail(subject, body):
    username = "bugsoda@gmail.com"
    password = ""
    recipient = "iychoi@email.arizona.edu"

    if username and password and recipient:
        try:
            session = login(username, password)
            sendmail(session, username, recipient, subject, body)
            session.close()
            print "Email Notification is sent to", recipient
        except:
            print "Failed to send an email Notification to", recipient

def go(k):
    create_index = True
    created_config = []
    for qalg in QUERY_ALGS:
        config = createConfig(k, qalg)
    	created_config.append(config)
        if create_index:
            createIndex(k, config)
            sizeTotal = calcIndexSize(config)
            print "index size"
            print "total size", "=", sizeTotal, "bytes"
            print "total size", "=", sizeTotal/1024, "kilobytes"
            print "total size", "=", sizeTotal/1024/1024, "megabytes"
            print "total size", "=", sizeTotal/1024/1024/1024, "gigabytes"

            create_index = False
            subject = "BioSpectra - index (" + str(k) +") is created"
            body = "BioSpectra - index (" + str(k) +") is created"
            trySendEmail(subject, body)
    
        classify(k, qalg, config)
        subject = "BioSpectra - classification (" + str(k) + ", " + qalg +") is done"
        body = "BioSpectra - classification (" + str(k) + ", " + qalg +") is done"
        trySendEmail(subject, body)

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
