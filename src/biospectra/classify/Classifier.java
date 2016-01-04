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
package biospectra.classify;

import biospectra.classify.beans.ClassificationResult;
import biospectra.classify.beans.SearchResultEntry;
import biospectra.Configuration;
import biospectra.index.IndexConstants;
import biospectra.lucene.KmerQueryAnalyzer;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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

/**
 *
 * @author iychoi
 */
public class Classifier implements Closeable {
    
    private static final Log LOG = LogFactory.getLog(Classifier.class);
    
    private File indexPath;
    private KmerQueryAnalyzer queryAnalyzer;
    private IndexReader indexReader;
    private IndexSearcher indexSearcher;
    private double minShouldMatch;
    private int kmerSize;
    private int kmerSkips;
    
    public Classifier(Configuration conf) throws Exception {
        if(conf == null) {
            throw new IllegalArgumentException("conf is null");
        }
        
        if(conf.getIndexPath() == null) {
            throw new IllegalArgumentException("indexPath is null");
        }
        
        if(conf.getKmerSize() <= 0) {
            throw new IllegalArgumentException("kmerSize must be larger than 0");
        }
        
        if(conf.getKmerSkips() <= 0) {
            throw new IllegalArgumentException("kmerSkips must be larger than 0");
        }
        
        initialize(new File(conf.getIndexPath()), conf.getKmerSize(), conf.getKmerSkips(), conf.getQueryMinShouldMatch());
    }
    
    private void initialize(File indexPath, int kmerSize, int kmerSkips, double minShouldMatch) throws Exception {
        if(!indexPath.exists() || !indexPath.isDirectory()) {
            throw new IllegalArgumentException("indexPath is not a directory or does not exist");
        }
        
        this.indexPath = indexPath;
        this.kmerSize = kmerSize;
        this.kmerSkips = kmerSkips;
        this.queryAnalyzer = new KmerQueryAnalyzer(this.kmerSize, this.kmerSkips);
        Directory dir = new MMapDirectory(this.indexPath.toPath()); 
        this.indexReader = DirectoryReader.open(dir);
        this.indexSearcher = new IndexSearcher(this.indexReader);
        this.minShouldMatch = minShouldMatch;
        
        BooleanQuery.setMaxClauseCount(10000);
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
                        pq.setSlop(this.kmerSkips * 2);
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
    
    protected BooleanQuery createQuery(KmerQueryAnalyzer analyzer, String field, String queryText, double minShouldMatch) {
        BooleanQuery proximityQuery = create2KgramFieldQuery(analyzer, field, queryText);
        BooleanQuery.Builder booleanQueryBuilder = new BooleanQuery.Builder();
        booleanQueryBuilder.setDisableCoord(proximityQuery.isCoordDisabled());
        booleanQueryBuilder.setMinimumNumberShouldMatch((int) (minShouldMatch * proximityQuery.clauses().size()));
        for (BooleanClause clause : proximityQuery) {
            booleanQueryBuilder.add(clause);
        }
        return booleanQueryBuilder.build();
    }
    
    public ClassificationResult classify(String header, String sequence) throws Exception {
        if(sequence == null || sequence.isEmpty()) {
            throw new IllegalArgumentException("sequence is null or empty");
        }
        
        ClassificationResult classificationResult = null;
        
        BooleanQuery q = createQuery(this.queryAnalyzer, IndexConstants.FIELD_SEQUENCE, sequence, this.minShouldMatch);
        
        int hitsPerPage = 10;
        TopScoreDocCollector collector = TopScoreDocCollector.create(hitsPerPage);
        this.indexSearcher.search(q, collector);
        TopDocs topdocs = collector.topDocs();
        ScoreDoc[] hits = topdocs.scoreDocs;
        
        if(hits.length > 0) {
            List<SearchResultEntry> resultArr = new ArrayList<SearchResultEntry>();

            double topscore = topdocs.getMaxScore();
            for(int i=0;i<hits.length;++i) {
                if(topscore - hits[i].score <= 1) {
                    int docId = hits[i].doc;
                    Document d = this.indexSearcher.doc(docId);
                    SearchResultEntry result = new SearchResultEntry(docId, d, i, hits[i].score);
                    resultArr.add(result);
                }
            }

            classificationResult = new ClassificationResult(header, sequence, resultArr);
        } else {
            classificationResult = new ClassificationResult(header, sequence, null);
        }
        
        return classificationResult;
    }
    
    @Override
    public void close() throws IOException {
        this.queryAnalyzer.close();
        this.indexReader.close();
    }
}
