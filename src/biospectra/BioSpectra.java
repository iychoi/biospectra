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

import biospectra.utils.FastaFileFinder;
import biospectra.utils.FastaFileReader;
import biospectra.utils.JsonSerializer;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Date;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.yeastrc.fasta.FASTAEntry;
import org.yeastrc.fasta.FASTAReader;

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
        }
    }

    private static void index(String[] args) throws Exception {
        String referenceDir = args[1];
        String indexDir = args[2];
        
        Date start = new Date(); 
        
        Indexer indexer = new Indexer(referenceDir, indexDir);
        indexer.index();
        
        Date end = new Date(); 
        LOG.info("indexing finished - " + (end.getTime() - start.getTime()) + " total milliseconds");
    }
    
    private static void search(String[] args) throws Exception {
        String indexDir = args[1];
        String field = args[2];
        String query = args[3];
        
        Searcher searcher = new Searcher(indexDir);
        
        Date start = new Date(); 
        
        List<SearchResult> result = searcher.search(field, query);
        
        Date end = new Date(); 
        LOG.info("searching finished - " + (end.getTime() - start.getTime()) + " total milliseconds");
            
        for(SearchResult r : result) {
            System.out.println(r.toString());
        }
    }
    
    private static void bulksearch(String[] args) throws Exception {
        String indexDir = args[1];
        String inputDir = args[2];
        String outputDir = args[3];
        
        Searcher searcher = new Searcher(indexDir);
        
        File output = new File(outputDir);
        if(!output.exists()) {
            output.mkdirs();
        }
        
        List<File> fastaDocs = FastaFileFinder.findFastaDocs(inputDir);
        for(File fastaDoc : fastaDocs) {
            Date start = new Date(); 
            
            FASTAReader reader = FastaFileReader.getFASTAReader(fastaDoc);
            FASTAEntry read = null;
            
            File resultOutput = new File(outputDir + "/" + fastaDoc.getName() + ".result");
            FileWriter fw = new FileWriter(resultOutput, false);
            BufferedWriter bw = new BufferedWriter(fw);
            
            int totalReads = 0;
            int classifiedReads = 0;
            int vagueReads = 0;
            int unknownReads = 0;
        
            while((read = reader.readNext()) != null) {
                String sequence = read.getSequence();
                
                List<SearchResult> result = searcher.search(sequence);
                BulkSearchResult bresult = new BulkSearchResult(sequence, result);
                JsonSerializer serializer = new JsonSerializer();
                String json = serializer.toJson(bresult);
                
                bw.write(json + "\n");
                
                totalReads++;
                switch(bresult.getType()) {
                    case VAGUE:
                        vagueReads++;
                        break;
                    case UNKNOWN:
                        unknownReads++;
                        break;
                    case CLASSIFIED:
                        classifiedReads++;
                        break;
                }
            }
            
            bw.close();
            
            Date end = new Date(); 
            LOG.info("searching " + fastaDoc.getAbsolutePath() + " finished - " + (end.getTime() - start.getTime()) + " total milliseconds");
            
            File sumResultOutput = new File(outputDir + "/" + fastaDoc.getName() + ".result.sum");
            FileWriter fws = new FileWriter(sumResultOutput, false);
            fws.write("total : " + totalReads + "\n");
            fws.write("classified : " + classifiedReads + "\n");
            fws.write("vague : " + vagueReads + "\n");
            fws.write("unknown : " + unknownReads + "\n");
            fws.close();
        }
    }
}
