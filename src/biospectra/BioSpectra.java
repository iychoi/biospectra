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

import java.io.File;
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
public class BioSpectra {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {
        search(args);
    }

    private static void index(String[] args) throws Exception {
        Indexer indexer = new Indexer("samples", "indices");
        
        indexer.index();
    }
    
    private static void search(String[] args) throws Exception {
        String index = "indices";
        String field = "sequence";
        //String queryString = "CACGGCTAGGTGGAAAGATT";
        String queryString = "AATCTTTCCACCTAGCCGTG";
        
        Analyzer analyzer = new KmerAnalyzer(20);
        Query q = new QueryParser(Version.LUCENE_40, field, analyzer).parse(queryString);
        
        Directory dir = FSDirectory.open(new File(index)); 
        IndexReader reader = IndexReader.open(dir);
        IndexSearcher searcher = new IndexSearcher(reader);
        
        int hitsPerPage = 10;
        TopScoreDocCollector collector = TopScoreDocCollector.create(hitsPerPage, true);
        searcher.search(q, collector);
        ScoreDoc[] hits = collector.topDocs().scoreDocs;
        
        System.out.println("Found " + hits.length + " hits.");
        for(int i=0;i<hits.length;++i) {
             int docId = hits[i].doc;
             Document d = searcher.doc(docId);
             System.out.println((i + 1) + ". " + d.get("filename"));
        }
    }
}
