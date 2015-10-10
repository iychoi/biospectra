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
import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

/**
 *
 * @author iychoi
 */
public class Searcher {
    
    private static final Log LOG = LogFactory.getLog(Searcher.class);
    
    private File indexPath;

    public static void main(String[] args) throws Exception {
        String indexPath = args[0];
        String field = args[1];
        String query = args[2];
        
        Searcher instance = new Searcher(indexPath);
        instance.search(field, query);
    }
    
    public Searcher(String indexPath) throws Exception {
        if(indexPath == null) {
            throw new IllegalArgumentException("indexPath is null");
        }
        
        initialize(new File(indexPath));
    }
    
    public Searcher(File indexPath) throws Exception {
        initialize(indexPath);
    }
    
    private void initialize(File indexPath) throws Exception {
        if(indexPath == null) {
            throw new IllegalArgumentException("indexPath is null");
        }
        
        this.indexPath = indexPath;
    }
    
    public List<SearchResult> search(String queryString) throws Exception {
        return search(IndexConstants.FIELD_SEQUENCE, queryString);
    }
    
    public List<SearchResult> search(String field, String queryString) throws Exception {
        Analyzer analyzer = new KmerAnalyzer(IndexConstants.KMERSIZE);
        Query q = new QueryParser(Version.LUCENE_40, field, analyzer).parse(queryString);
        
        Directory dir = FSDirectory.open(this.indexPath); 
        IndexReader reader = IndexReader.open(dir);
        IndexSearcher searcher = new IndexSearcher(reader);
        
        int hitsPerPage = 10;
        TopScoreDocCollector collector = TopScoreDocCollector.create(hitsPerPage, true);
        searcher.search(q, collector);
        ScoreDoc[] hits = collector.topDocs().scoreDocs;
        
        List<SearchResult> resultArr = new ArrayList<SearchResult>();
        
        double topscore = 0;
        for(int i=0;i<hits.length;++i) {
            if (i == 0) {
                topscore = hits[i].score;
            }
            
            if(topscore - hits[i].score <= 1) {
                int docId = hits[i].doc;
                Document d = searcher.doc(docId);
                SearchResult result = new SearchResult(docId, d, i, hits[i].score);
                resultArr.add(result);
            }
        }
        
        return resultArr;
    }
}
