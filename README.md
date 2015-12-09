# biospectra
BioSpectra: A Metagenomic Read Classification System

BUILD
-----
System Requirements: Java 1.7.0, Ant

This code uses "Ant" build system for compilation. So you may need to install "Ant" build system before you start compilation.
Please type "ant" in the root of this package (where "build.xml" exists).

Jar package will be generated under "/dist" path.
All dependencies(lucene 4.0) will also be copied to "/dist/lib" path after the compilation.


EXECUTION
---------

Index Construction

```
biospectra_indexing.sh <path_to_reference_FASTA_files> <path_of_index>
```

BioSpectra accepts input FASTA path as a directory. In this case, BioSpectra will recursively find all FASTA files and process.


Classification

```
biospectra_classify.sh <path_of_index> <path_to_query_FASTA_files> <path_to_output_dir>
```

BioSpectra accepts query FASTA path as a directory. In this case, BioSpectra will recursively find all FASTA files and process.
Classification results (in form of json) will be stored as a file with the same filename as input with a extension ".result".  Also it automatically generates a summary file that shows timetaken of classifications and total number of "classified", "vague" or "unknown" reads. 


BENCHMARK DATASETS
------------------

Source package contains a script to download NCBI bacteria reference datasets at /benchmark/down_ncbi_bacteria_reference.sh. To work with this datasets, you will need at least 25GB of disk space including size of generated index form this datasets.

A simulated query datasets extracted from NCBI bacteria reference datasets with fixed error ratio is provided at /benchmark/bacteria_ncbi_simulated directory. For example, a /benchmark/bacteria_ncbi_simulated/sample_0.04.fa file contains reads (each in length about 100) randomly extracted from NCBI bacteria datasets with 4% of sequencing errors at random positions.

Another simulated query datasets extracted from FAMeS(Fidelity of Analysis of Metagenomic Samples) datasets are provided at /benchmark/simHC.20.500 directory. Each file in the directory contains 500 reads that are extracted by a single species (referring a filename).


