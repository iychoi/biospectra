/*
 * Copyright 2015 iychoi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package biospectra;

import biospectra.search.SearchResult;
import biospectra.search.SequenceSearcher;
import biospectra.index.IndexUtil;
import biospectra.index.Indexer;
import biospectra.taxdb.TaxonDB;
import biospectra.taxdb.Taxonomy;
import biospectra.utils.FastaFileHelper;
import java.io.File;
import java.util.Date;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class BioSpectra {

    private static final Log LOG = LogFactory.getLog(BioSpectra.class);
    
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {
        String op = args[0];
        if(op.equalsIgnoreCase("i") || op.equalsIgnoreCase("index")) {
            index(args);
        } else if(op.equalsIgnoreCase("s") || op.equalsIgnoreCase("search")) {
            search(args);
        } else if(op.equalsIgnoreCase("bs") || op.equalsIgnoreCase("bsearch")) {
            bulksearch(args);
        } else if(op.equalsIgnoreCase("u") || op.equalsIgnoreCase("utils")) {
            utils(args);
        }
    }

    private static void index(String[] args) throws Exception {
        String referenceDir = args[1];
        String indexDir = args[2];
        
        Configuration conf = new Configuration();
        conf.setIndexPath(indexDir);
        
        if(args.length >= 4) {
            conf.setKmerSize(Integer.parseInt(args[3]));
        }
        
        Indexer indexer = new Indexer(conf);
        
        List<File> refereneFiles = FastaFileHelper.findFastaDocs(referenceDir);
        for(File fastaDoc : refereneFiles) {
            LOG.info("indexing " + fastaDoc.getAbsolutePath() + " started");
            Date start = new Date();
            
            indexer.index(fastaDoc);
            
            Date end = new Date(); 
            LOG.info("indexing " + fastaDoc.getAbsolutePath() + " finished - " + (end.getTime() - start.getTime()) + " total milliseconds");
        }
        
        indexer.close();
    }
    
    private static void search(String[] args) throws Exception {
        String indexDir = args[1];
        String query = args[2];
        
        Configuration conf = new Configuration();
        conf.setIndexPath(indexDir);
        
        if(args.length >= 4) {
            conf.setKmerSize(Integer.parseInt(args[3]));
        }
        
        SequenceSearcher searcher = new SequenceSearcher(conf);
        
        Date start = new Date(); 
        
        List<SearchResult> result = searcher.search(query);
        
        Date end = new Date(); 
        LOG.info("searching finished - " + (end.getTime() - start.getTime()) + " total milliseconds");
        
        boolean hasResult = false;
        for(SearchResult r : result) {
            System.out.println(r.toString());
            hasResult = true;
        }
        
        if(!hasResult) {
            System.out.println("no document found");
        }
        
        searcher.close();
    }
    
    private static void bulksearch(String[] args) throws Exception {
        String indexDir = args[1];
        String inputDir = args[2];
        String outputDir = args[3];
        
        Configuration conf = new Configuration();
        conf.setIndexPath(indexDir);
        
        if(args.length >= 5) {
            conf.setKmerSize(Integer.parseInt(args[4]));
        }
        
        SequenceSearcher searcher = new SequenceSearcher(conf);
        
        File output = new File(outputDir);
        if(!output.exists()) {
            output.mkdirs();
        }
        
        List<File> fastaDocs = FastaFileHelper.findFastaDocs(inputDir);
        for(File fastaDoc : fastaDocs) {
            File resultOutput = new File(outputDir + "/" + fastaDoc.getName() + ".result");
            File sumResultOutput = new File(outputDir + "/" + fastaDoc.getName() + ".result.sum");
            searcher.bulkSearch(fastaDoc, resultOutput, sumResultOutput);
        }
        
        searcher.close();
    }

    private static void utils(String[] args) throws Exception {
        String operation = args[1];
        
        if(operation.equalsIgnoreCase("index")) {
            String indexDir = args[2];
        
            IndexUtil indexutil = new IndexUtil(indexDir);
            System.out.println("total docs : " + indexutil.countDocs());
            indexutil.close();
        } else if(operation.equalsIgnoreCase("fasta")) {
            String fastaDir = args[2];
        
            int count = 0;
            long size_sum = 0;
            List<File> fastaDocs = FastaFileHelper.findFastaDocs(fastaDir);
            for(File fastaDoc : fastaDocs) {
                System.out.println("found FASTA file : " + fastaDoc.getAbsolutePath());
                System.out.println("size> " + fastaDoc.length());
                count++;
                size_sum += fastaDoc.length();
            }
            System.out.println("SUMMARY");
            System.out.println("Count : " + count);
            System.out.println("Size in total : " + size_sum);
        } else if(operation.equalsIgnoreCase("nfasta")) {
            String fastaDir = args[2];
        
            int count = 0;
            long size_sum = 0;
            List<File> fastaDocs = FastaFileHelper.findNonFastaDocs(fastaDir);
            for(File fastaDoc : fastaDocs) {
                System.out.println("found non-FASTA file : " + fastaDoc.getAbsolutePath());
                System.out.println("size> " + fastaDoc.length());
                count++;
                size_sum += fastaDoc.length();
            }
            System.out.println("SUMMARY");
            System.out.println("Count : " + count);
            System.out.println("Size in total : " + size_sum);
        } else if(operation.equalsIgnoreCase("taxid")) {
            String dbDir = args[2];
            int taxid = Integer.parseInt(args[3]);
            
            TaxonDB db = new TaxonDB(dbDir);
            List<Taxonomy> taxon = db.getFullTaxonomyHierarchyByTaxid(taxid);
            for(Taxonomy t : taxon) {
                System.out.println(t.toString());
            }
            db.close();
        } else if(operation.equalsIgnoreCase("gi")) {
            String dbDir = args[2];
            int gi = Integer.parseInt(args[3]);
            
            TaxonDB db = new TaxonDB(dbDir);
            List<Taxonomy> taxon = db.getFullTaxonomyHierarchyByGI(gi);
            for(Taxonomy t : taxon) {
                System.out.println(t.toString());
            }
            db.close();
        }
    }
}
