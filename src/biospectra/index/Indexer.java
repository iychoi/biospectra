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
package biospectra.index;

import biospectra.Configuration;
import biospectra.lucene.KmerIndexAnalyzer;
import biospectra.utils.FastaFileHelper;
import biospectra.utils.FastaFileReader;
import biospectra.utils.SequenceHelper;
import java.io.Closeable;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.yeastrc.fasta.FASTAEntry;
import org.yeastrc.fasta.FASTAReader;

/**
 *
 * @author iychoi
 */
public class Indexer implements Closeable {
    
    private static final Log LOG = LogFactory.getLog(Indexer.class);
    
    private File indexPath;
    private Analyzer analyzer;
    private IndexWriter indexWriter;
    
    public Indexer(Configuration conf) throws Exception {
        if(conf == null) {
            throw new IllegalArgumentException("conf is null");
        }
        
        if(conf.getIndexPath() == null) {
            throw new IllegalArgumentException("indexPath is null");
        }
        
        if(conf.getKmerSize() <= 0) {
            throw new IllegalArgumentException("kmerSize must be larger than 0");
        }
        
        initialize(new File(conf.getIndexPath()), conf.getKmerSize(), conf.getScoringAlgorithmObject());
    }
    
    private void initialize(File indexPath, int kmerSize, Similarity similarity) throws Exception {
        if(!indexPath.exists()) {
            indexPath.mkdirs();
        }
        
        if(indexPath.exists()) {
            cleanUpDirectory(indexPath);
        }
        
        this.indexPath = indexPath;
        this.analyzer = new KmerIndexAnalyzer(kmerSize);
        Directory dir = new NIOFSDirectory(this.indexPath.toPath()); 
        IndexWriterConfig config = new IndexWriterConfig(this.analyzer); 
        if(similarity != null) {
            config.setSimilarity(similarity);
        }
        
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        this.indexWriter = new IndexWriter(dir, config);
    }
    
    public void index(List<File> fastaDocs) throws Exception {
        if(fastaDocs == null) {
            throw new IllegalArgumentException("fastaDocs is null");
        }
        
        for(File fastaDoc : fastaDocs) {
            index(fastaDoc);
        }
    }
    
    public void index(File fastaDoc) throws Exception {
        if(fastaDoc == null) {
            throw new IllegalArgumentException("fastaDoc is null");
        }
        
        File taxonDoc = FastaFileHelper.findTaxonHierarchyDoc(fastaDoc);
        index(fastaDoc, taxonDoc);
    }
    
    public void index(File fastaDoc, File taxonDoc) throws Exception {
        if(fastaDoc == null) {
            throw new IllegalArgumentException("fastaDoc is null");
        }
        
        String taxonTree = "";
        
        if(taxonDoc != null && taxonDoc.exists()) {
            FileReader reader = new FileReader(taxonDoc);
            taxonTree = IOUtils.toString(reader);
            IOUtils.closeQuietly(reader);
        }
        
        FASTAReader reader = FastaFileReader.getFASTAReader(fastaDoc);
        FASTAEntry read = null;
        
        Document doc = new Document();
        Field filenameField = new StringField(IndexConstants.FIELD_FILENAME, fastaDoc.getName(), Field.Store.YES);
        Field headerField = new StringField(IndexConstants.FIELD_HEADER, "", Field.Store.YES);
        Field sequenceDirectionField = new StringField(IndexConstants.FIELD_SEQUENCE_DIRECTION, "", Field.Store.YES);
        Field taxonTreeField = new StringField(IndexConstants.FIELD_TAXONOMY_TREE, taxonTree, Field.Store.YES);
        Field sequenceField = new TextField(IndexConstants.FIELD_SEQUENCE, "", Field.Store.NO);
        
        doc.add(filenameField);
        doc.add(headerField);
        doc.add(sequenceDirectionField);
        doc.add(taxonTreeField);
        doc.add(sequenceField);
        
        while((read = reader.readNext()) != null) {
            String headerLine = read.getHeaderLine();
            if(headerLine.startsWith(">")) {
                headerLine = headerLine.substring(1);
            }
            
            String sequence = read.getSequence();
            headerField.setStringValue(headerLine);
            
            // forward-strand
            sequenceDirectionField.setStringValue("forward");
            sequenceField.setStringValue(sequence);
            this.indexWriter.addDocument(doc);
            
            // reverse-strand
            sequenceDirectionField.setStringValue("reverse");
            sequenceField.setStringValue(SequenceHelper.getReverseComplement(sequence));
            this.indexWriter.addDocument(doc);
        }
        
        reader.close();
    }
    
    @Override
    public void close() throws IOException {
        this.analyzer.close();
        this.indexWriter.close();
    }

    private void cleanUpDirectory(File indexPath) {
        File[] listFiles = indexPath.listFiles();
        for(File f : listFiles) {
            if(f.isFile()) {
                f.delete();
            } else {
                //remove recursively
                cleanUpDirectory(f);
                f.delete();
            }
        }
    }
}
