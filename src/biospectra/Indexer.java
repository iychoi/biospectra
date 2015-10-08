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
import java.io.File;
import java.util.Date;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.yeastrc.fasta.FASTAEntry;
import org.yeastrc.fasta.FASTAReader;

/**
 *
 * @author iychoi
 */
public class Indexer {
    
    private static final Log LOG = LogFactory.getLog(Indexer.class);
    
    private List<File> fastaDocs;
    private File indexPath;
    
    public static void main(String[] args) throws Exception {
        String docPath = args[0];
        String indexPath = args[1];
        
        Indexer instance = new Indexer(docPath, indexPath);
        instance.index();
    }
    
    public Indexer(String docPath, String indexPath) throws Exception {
        if(docPath == null) {
            throw new IllegalArgumentException("docPath is null");
        }
        
        if(indexPath == null) {
            throw new IllegalArgumentException("indexPath is null");
        }
        
        initialize(new File(docPath), new File(indexPath));
    }
    
    public Indexer(File docPath, File indexPath) throws Exception {
        initialize(docPath, indexPath);
    }
    
    private void initialize(File docPath, File indexPath) throws Exception {
        if(docPath == null) {
            throw new IllegalArgumentException("docPath is null");
        }
        
        if(indexPath == null) {
            throw new IllegalArgumentException("indexPath is null");
        }
        
        this.fastaDocs = FastaFileFinder.findFastaDocs(docPath);
        this.indexPath = indexPath;
    }
    
    public void index() throws Exception {
        LOG.info("indexing...");
        
        Date start = new Date();
        
        Directory dir = FSDirectory.open(this.indexPath); 
        Analyzer analyzer = new KmerAnalyzer(IndexConstants.KMERSIZE);
        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_40, analyzer); 

        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        
        // indexing
        IndexWriter writer = new IndexWriter(dir, config);
        for(File fastaDoc : this.fastaDocs) {
            indexDocs(writer, fastaDoc);
        }
        writer.close();
        
        Date end = new Date(); 
        
        LOG.info("indexing finished - " + (end.getTime() - start.getTime()) + " total milliseconds");
    }
    
    private void indexDocs(IndexWriter writer, File fastaDoc) throws Exception {
        if(writer == null) {
            throw new IllegalArgumentException("writer is null");
        }
        
        if(fastaDoc == null) {
            throw new IllegalArgumentException("fastaDoc is null");
        }
        
        LOG.info("indexing " + fastaDoc.getAbsolutePath());
        
        FASTAReader reader = FastaFileReader.getFASTAReader(fastaDoc);
        FASTAEntry read = null;
        
        while((read = reader.readNext()) != null) {
            Document doc = new Document();
            
            Field filenameField = new StringField(IndexConstants.FIELD_FILENAME, fastaDoc.getName(), Field.Store.YES);
            //LOG.debug("filename: " + fastaDoc.getName()); 
            doc.add(filenameField); 
            
            String headerLine = read.getHeaderLine();
            //LOG.debug("header: " + headerLine); 
            if(headerLine.startsWith(">")) {
                headerLine = headerLine.substring(1);
            }
            
            doc.add(new StringField(IndexConstants.FIELD_HEADER, headerLine, Field.Store.YES));
            
            String sequence = read.getSequence();
            //LOG.debug("sequence: " + sequence); 
            doc.add(new TextField(IndexConstants.FIELD_SEQUENCE, sequence, Field.Store.NO));
            
            //if(writer.getConfig().getOpenMode() == IndexWriterConfig.OpenMode.CREATE) {
                writer.addDocument(doc);
            //} else {
            //    writer.updateDocument(new Term("filename", fastaDoc.getName()), doc);
            //}
        }
        
        reader.close();
    }
}
