echo "************************************"
echo "Downloading NCBI Bacteria References"
echo "************************************"

wget ftp://ftp.ncbi.nih.gov/genomes/Bacteria/all.fna.tar.gz
tar -zxf all.fna.tar.gz
rm all.fna.tar.gz
