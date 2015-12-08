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
package biospectra.search;

import biospectra.Configuration;
import biospectra.index.IndexConstants;
import biospectra.lucene.KmerQueryAnalyzer;
import biospectra.search.BulkSearchResult.SearchResultType;
import biospectra.utils.FastaFileReader;
import biospectra.utils.JsonSerializer;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.analysis.CachingTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;
import org.yeastrc.fasta.FASTAEntry;
import org.yeastrc.fasta.FASTAReader;

/**
 *
 * @author iychoi
 */
public class SequenceSearcher implements Closeable {
    
    private static final Log LOG = LogFactory.getLog(SequenceSearcher.class);
    
    private File indexPath;
    private KmerQueryAnalyzer queryAnalyzer;
    private IndexReader indexReader;
    private IndexSearcher indexSearcher;
    private double minShouldMatch;
    private int kmerSize;
    
    public SequenceSearcher(File indexPath, int kmerSize) throws Exception {
        if(indexPath == null) {
            throw new IllegalArgumentException("indexPath is null");
        }
        
        if(kmerSize <= 0) {
            throw new IllegalArgumentException("kmerSize must be larger than 0");
        }
        
        initialize(indexPath, kmerSize, Configuration.DEFAULT_QUERY_TERMS_MIN_SHOULD_MATCH);
    }
    
    public SequenceSearcher(Configuration conf) throws Exception {
        if(conf == null) {
            throw new IllegalArgumentException("conf is null");
        }
        
        if(conf.getIndexPath() == null) {
            throw new IllegalArgumentException("indexPath is null");
        }
        
        if(conf.getKmerSize() <= 0) {
            throw new IllegalArgumentException("kmerSize must be larger than 0");
        }
        
        initialize(new File(conf.getIndexPath()), conf.getKmerSize(), conf.getQueryMinShouldMatch());
    }
    
    private void initialize(File indexPath, int kmerSize, double minShouldMatch) throws Exception {
        if(!indexPath.exists() || !indexPath.isDirectory()) {
            throw new IllegalArgumentException("indexPath is not a directory or does not exist");
        }
        
        this.indexPath = indexPath;
        this.kmerSize = kmerSize;
        this.queryAnalyzer = new KmerQueryAnalyzer(this.kmerSize, this.kmerSize/2);
        Directory dir = new MMapDirectory(this.indexPath.toPath()); 
        this.indexReader = DirectoryReader.open(dir);
        this.indexSearcher = new IndexSearcher(this.indexReader);
        this.minShouldMatch = minShouldMatch;
        
        BooleanQuery.setMaxClauseCount(10000);
    }
    
    public List<SearchResult> search(String sequence) throws Exception {
        return search(sequence, this.minShouldMatch);
    }
    
    protected final BooleanQuery create2KgramFieldQuery(KmerQueryAnalyzer analyzer, String field, String queryText) {
        try (TokenStream source = analyzer.tokenStream(field, queryText);
            CachingTokenFilter stream = new CachingTokenFilter(source)) {

            TermToBytesRefAttribute termAtt = stream.getAttribute(TermToBytesRefAttribute.class);
            
            if (termAtt == null) {
                return null;
            }

            // phase 1: read through the stream and assess the situation:
            // counting the number of tokens/positions and marking if we have any synonyms.
            int numTokens = 0;

            stream.reset();
            while (stream.incrementToken()) {
                numTokens++;
            }

            // phase 2: based on token count, presence of synonyms, and options
            // formulate a single term, boolean, or phrase.
            if (numTokens == 0) {
                return null;
            } else if (numTokens == 1) {
                // single term
                return null;
            } else {
                Term termArr[] = new Term[2];
                for(int i=0;i<2;i++) {
                    termArr[i] = null;
                }

                BooleanQuery.Builder q = new BooleanQuery.Builder();
                q.setDisableCoord(false);
                
                TermToBytesRefAttribute termAttB = stream.getAttribute(TermToBytesRefAttribute.class);

                stream.reset();
                int count = 0;
                while (stream.incrementToken()) {
                    Term t = new Term(field, BytesRef.deepCopyOf(termAttB.getBytesRef()));
                    if(count % 2 == 0) {
                        termArr[0] = t;
                    } else {
                        termArr[1] = t;
                        
                        PhraseQuery.Builder pq = new PhraseQuery.Builder();
                        pq.setSlop(analyzer.getSkips() * 2);
                        pq.add(termArr[0]);
                        pq.add(termArr[1]);
                        
                        q.add(pq.build(), BooleanClause.Occur.SHOULD);
                        
                        termArr[0] = null;
                        termArr[1] = null;
                    }
                    
                    count++;
                }

                return q.build();
            }
        } catch (IOException e) {
            throw new RuntimeException("Error analyzing query text", e);
        }
    }
    
    private BooleanQuery createQuery(KmerQueryAnalyzer analyzer, String field, String queryText, double minShouldMatch) {
        BooleanQuery proximityQuery = create2KgramFieldQuery(analyzer, field, queryText);
        BooleanQuery.Builder booleanQueryBuilder = new BooleanQuery.Builder();
        booleanQueryBuilder.setDisableCoord(proximityQuery.isCoordDisabled());
        booleanQueryBuilder.setMinimumNumberShouldMatch((int) (minShouldMatch * proximityQuery.clauses().size()));
        for (BooleanClause clause : proximityQuery) {
            booleanQueryBuilder.add(clause);
        }
        return booleanQueryBuilder.build();
    }
    
    public List<SearchResult> search(String sequence, double minShouldMatch) throws Exception {
        if(sequence == null || sequence.isEmpty()) {
            throw new IllegalArgumentException("sequence is null or empty");
        }
        
        BooleanQuery q = createQuery(this.queryAnalyzer, IndexConstants.FIELD_SEQUENCE, sequence, minShouldMatch);
        
        int hitsPerPage = 10;
        TopScoreDocCollector collector = TopScoreDocCollector.create(hitsPerPage);
        this.indexSearcher.search(q, collector);
        TopDocs topdocs = collector.topDocs();
        ScoreDoc[] hits = topdocs.scoreDocs;
        
        List<SearchResult> resultArr = new ArrayList<SearchResult>();
        
        double topscore = topdocs.getMaxScore();
        for(int i=0;i<hits.length;++i) {
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
        bulkSearch(inputFasta, classifyOutput, summaryOutput, this.minShouldMatch);
    }
    
    public void bulkSearch(File inputFasta, File classifyOutput, File summaryOutput, double minShouldMatch) throws Exception {
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
        summary.setQueryFilename(inputFasta.getName());
        summary.setStartTime(new Date());

        int threads = 4;
        int queueSize = 1000;
        ExecutorService executor = new ThreadPoolExecutor(threads, queueSize, 5000L, TimeUnit.MILLISECONDS, 
                new ArrayBlockingQueue<Runnable>(queueSize, true), 
                new ThreadPoolExecutor.CallerRunsPolicy());
        
        final double _minShouldMatch = minShouldMatch;
        while((read = reader.readNext()) != null) {
            final String sequence = read.getSequence();
            final String header = read.getHeaderLine();

            Runnable worker = new Runnable() {

                @Override
                public void run() {
                    try {
                        BooleanQuery q = createQuery(queryAnalyzer, IndexConstants.FIELD_SEQUENCE, sequence, _minShouldMatch);
                        
                        int hitsPerPage = 10;
                        TopScoreDocCollector collector = TopScoreDocCollector.create(hitsPerPage);
                        indexSearcher.search(q, collector);
                        TopDocs topdocs = collector.topDocs();
                        ScoreDoc[] hits = topdocs.scoreDocs;
                        
                        BulkSearchResult bresult = null;
                        if(hits.length > 0) {
                            List<SearchResult> resultArr = new ArrayList<SearchResult>();

                            double topscore = topdocs.getMaxScore();
                            for(int i=0;i<hits.length;++i) {
                                if(topscore - hits[i].score <= 1) {
                                    int docId = hits[i].doc;
                                    Document d = indexSearcher.doc(docId);
                                    SearchResult result = new SearchResult(docId, d, i, hits[i].score);
                                    resultArr.add(result);
                                }
                            }
                            
                            bresult = new BulkSearchResult(header, sequence, resultArr);
                        } else {
                            bresult = new BulkSearchResult(header, sequence, null);
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
                        LOG.error("Exception occurred during search", ex);
                    }
                }
            };
            executor.submit(worker);
        }
        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

        bw.close();

        summary.setEndTime(new Date());
        LOG.info("bulk searching of " + summary.getQueryFilename() + " finished in " + summary.getTimeTaken() + " millisec");
        
        if(summaryOutput != null) {
            summary.saveTo(summaryOutput);
        }
    }

    @Override
    public void close() throws IOException {
        this.queryAnalyzer.close();
        this.indexReader.close();
    }
}
