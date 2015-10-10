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

import biospectra.lucene.KmerAnalyzer;
import biospectra.utils.FastaFileReader;
import biospectra.utils.JsonSerializer;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.Version;
import org.yeastrc.fasta.FASTAEntry;
import org.yeastrc.fasta.FASTAReader;

/**
 *
 * @author iychoi
 */
public class SequenceSearcher implements Closeable {
    private static final Log LOG = LogFactory.getLog(SequenceSearcher.class);
    
    private File indexPath;
    private Analyzer analyzer;
    private IndexReader indexReader;
    private IndexSearcher indexSearcher;
    
    public SequenceSearcher(String indexPath) throws Exception {
        if(indexPath == null) {
            throw new IllegalArgumentException("indexPath is null");
        }
        
        initialize(new File(indexPath));
    }
    
    public SequenceSearcher(File indexPath) throws Exception {
        initialize(indexPath);
    }
    
    private void initialize(File indexPath) throws Exception {
        if(indexPath == null) {
            throw new IllegalArgumentException("indexPath is null");
        }
        
        this.indexPath = indexPath;
        this.analyzer = new KmerAnalyzer(IndexConstants.KMERSIZE);
        Directory dir = new NIOFSDirectory(this.indexPath); 
        this.indexReader = DirectoryReader.open(dir);
        this.indexSearcher = new IndexSearcher(this.indexReader);
    }
    
    public List<SearchResult> search(String sequence) throws Exception {
        if(sequence == null || sequence.isEmpty()) {
            throw new IllegalArgumentException("sequence is null or empty");
        }
        
        QueryParser queryParser = new QueryParser(Version.LUCENE_40, IndexConstants.FIELD_SEQUENCE, this.analyzer);
        Query q = queryParser.parse(sequence);
        
        int hitsPerPage = 10;
        TopScoreDocCollector collector = TopScoreDocCollector.create(hitsPerPage, true);
        this.indexSearcher.search(q, collector);
        ScoreDoc[] hits = collector.topDocs().scoreDocs;
        
        List<SearchResult> resultArr = new ArrayList<SearchResult>();
        
        double topscore = 0;
        for(int i=0;i<hits.length;++i) {
            if (i == 0) {
                topscore = hits[i].score;
            }
            
            if(topscore - hits[i].score <= 1) {
                int docId = hits[i].doc;
                Document d = this.indexSearcher.doc(docId);
                SearchResult result = new SearchResult(docId, d, i, hits[i].score);
                resultArr.add(result);
            }
        }
        
        return resultArr;
    }
    
    public void bulkSearch(File inputFasta, File classifyOutput, File summaryOutput) throws Exception {
        if(inputFasta == null) {
            throw new IllegalArgumentException("inputFasta is null");
        }
        
        if(classifyOutput == null) {
            throw new IllegalArgumentException("classifyOutput is null");
        }
        
        if(!classifyOutput.getParentFile().exists()) {
            classifyOutput.getParentFile().mkdirs();
        }
        
        if(summaryOutput != null) {
            if(!summaryOutput.getParentFile().exists()) {
                summaryOutput.getParentFile().mkdirs();
            }
        }
        
        FASTAReader reader = FastaFileReader.getFASTAReader(inputFasta);
        FASTAEntry read = null;

        FileWriter fw = new FileWriter(classifyOutput, false);
        final BufferedWriter bw = new BufferedWriter(fw, 1024*1024);

        final BulkSearchResultSummary summary = new BulkSearchResultSummary();

        int threads = 4;
        int queueSize = 1000;
        ExecutorService executor = new ThreadPoolExecutor(threads, queueSize, 5000L, TimeUnit.MILLISECONDS, 
                new ArrayBlockingQueue<Runnable>(queueSize, true), 
                new ThreadPoolExecutor.CallerRunsPolicy());
        
        long n = 0;
        while((read = reader.readNext()) != null) {
            final String sequence = read.getSequence();

            Runnable worker = new Runnable() {

                @Override
                public void run() {
                    try {
                        QueryParser queryParser = new QueryParser(Version.LUCENE_40, IndexConstants.FIELD_SEQUENCE, analyzer);
                        Query q = queryParser.parse(sequence);
                        
                        int hitsPerPage = 10;
                        TopScoreDocCollector collector = TopScoreDocCollector.create(hitsPerPage, true);
                        indexSearcher.search(q, collector);
                        ScoreDoc[] hits = collector.topDocs().scoreDocs;
                        
                        BulkSearchResult bresult = null;
                        if(hits.length > 0) {
                            List<SearchResult> resultArr = new ArrayList<SearchResult>();

                            double topscore = 0;
                            for(int i=0;i<hits.length;++i) {
                                if (i == 0) {
                                    topscore = hits[i].score;
                                }

                                if(topscore - hits[i].score <= 1) {
                                    int docId = hits[i].doc;
                                    Document d = indexSearcher.doc(docId);
                                    SearchResult result = new SearchResult(docId, d, i, hits[i].score);
                                    resultArr.add(result);
                                }
                            }
                            
                            bresult = new BulkSearchResult(sequence, resultArr);
                        } else {
                            bresult = new BulkSearchResult(sequence, null);
                        }
                        
                        JsonSerializer serializer = new JsonSerializer();
                        String json = serializer.toJson(bresult);
                        
                        synchronized(summary) {
                            summary.report(bresult);
                        }
                        synchronized(bw) {
                            bw.write(json + "\n");
                        }
                    } catch (Exception ex) {
                        LOG.error(ex);
                    }
                }
            };
            executor.submit(worker);
            //System.out.println("worker" + n + " submitted");
            n++;
        }
        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

        bw.close();

        if(summaryOutput != null) {
            summary.saveTo(summaryOutput);
        }
    }

    @Override
    public void close() throws IOException {
        this.analyzer.close();
        this.indexReader.close();
    }
}
