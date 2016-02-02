#! /usr/bin/env python

import os
import os.path
import sys
import json

KNOWN_ANSWERS_SIMHC = {
    "637000047.fna":["Burkholderia", "Burkholderia ambifaria", "Burkholderia ambifaria AMMD"],
    "637000208.fna":["Polaromonas", "Polaromonas sp. JS666"],
    "639633063.fna":["Syntrophobacter", "Syntrophobacter fumaroxidans", "Syntrophobacter fumaroxidans MPOB"],
    "637000088.fna":["Dechloromonas", "Dechloromonas aromatica", "Dechloromonas aromatica RCB"],
    "637000216.fna":["Pseudoalteromonas", "Pseudoalteromonas atlantica", "Pseudoalteromonas atlantica T6c"],
    "640069309.fna":["Clostridium", "Clostridium thermocellum", "Clostridium thermocellum ATCC 27405"],
    "637000116.fna":["Frankia", "Frankia sp. CcI3"],
    "637000221.fna":["Pseudomonas", "Pseudomonas fluorescens", "Pseudomonas fluorescens PfO-1"],
    "640069327.fna":["Rhodobacter", "Rhodobacter sphaeroides", "Rhodobacter sphaeroides 2.4.1"],
    "637000119.fna":["Geobacter", "Geobacter metallireducens", "Geobacter metallireducens GS-15"],
    "637000237.fna":["Rhodopseudomonas", "Rhodopseudomonas palustris", "Rhodopseudomonas palustris BisB18"],
    "640427103.fna":["Bradyrhizobium", "Bradyrhizobium sp. BTAi1"],
    "637000160.fna":["Chelativorans", "Chelativorans sp. BNC1"],
    "637000260.fna":["Shewanella", "Shewanella sp. MR-7"],
    "640753002.fna":["Alkaliphilus", "Alkaliphilus metalliredigens", "Alkaliphilus metalliredigens QYMF"],
    "637000162.fna":["Methanosarcina", "Methanosarcina barkeri", "Methanosarcina barkeri Fusaro"],
    "639633037.fna":["Marinobacter", "Marinobacter aquaeolei", "Marinobacter aquaeolei VT8"],
    "643348537.fna":["Desulfitobacterium", "Desulfitobacterium hafniense", "Desulfitobacterium hafniense DCB-2"],
    "637000192.fna":["Nitrobacter", "Nitrobacter hamburgensis", "Nitrobacter hamburgensis UNDEF", "Nitrobacter hamburgensis X14"],
    "639633046.fna":["Nocardioides", "Nocardioides sp. JS614"]
}

def _compute_accuracy(path):
    if path.endswith(".result"):
        print "parsing", path

        fasta_file = path[:-7]
        fasta_file = os.path.basename(fasta_file)

        classified = 0
        vague = 0
        unknown = 0
        tp = 0
        fp = 0
        fn = 0

        with open(path) as f:
            for line in f:
                j = json.loads(line)
                ctype = j['type']
                result = j['result']
                taxon_name = j['taxon_name']
                taxon_rank = j['taxon_rank']

                if ctype == "CLASSIFIED":
                    classified += 1
                    found = False
                    for r in result:
                        taxon_hier = r['taxon_hierarchy']

                        if taxon_hier and len(taxon_hier) > 0:
                            hj = json.loads(taxon_hier)
                            for h in hj:
                                if h['name'] in KNOWN_ANSWERS_SIMHC[fasta_file]:
                                    found = True
                                    break

                        if found:
                            break

                    if found:
                        tp += 1
                    else:
                        fp += 1
                        
    
                elif ctype == "VAGUE":
                    vague += 1
                    fn += 1
                elif ctype == "UNKNOWN":
                    unknown += 1
                    fn += 1
                    
        return classified, vague, unknown, tp, fp, fn
    else:
        return 0, 0, 0, 0, 0, 0

def compute_accuracy(path):
    classified = 0
    vague = 0
    unknown = 0
    tp = 0
    fp = 0
    fn = 0

    if os.path.isdir(path):
        for p in os.listdir(path):
            _c, _v, _u, _tp, _fp, _fn = compute_accuracy(os.path.join(path, p))
            classified += _c
            vague += _v
            unknown += _u
            tp += _tp
            fp += _fp
            fn += _fn
    else:
        _c, _v, _u, _tp, _fp, _fn = _compute_accuracy(path)
        classified += _c
        vague += _v
        unknown += _u
        tp += _tp
        fp += _fp
        fn += _fn

    return classified, vague, unknown, tp, fp, fn

def main():
    classified, vague, unknown, tp, fp, fn = compute_accuracy(sys.argv[1])
    print "classified:", classified
    print "vague:", vague
    print "unknown:", unknown
    print "true-positive (Correct):", tp
    print "false-positive (Wrong):", fp
    print "false-negative (Miss):", fn


if __name__ == "__main__":
    main()
