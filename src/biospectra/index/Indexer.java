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
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
    private int workerThreads = 1;
    private ExecutorService executor;
    private Queue<Document> freeQueue = new ConcurrentLinkedQueue<Document>();
    
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
        
        if(conf.getWorkerThreads() <= 0) {
            throw new IllegalArgumentException("workerThreads must be larger than 0");
        }
        
        initialize(new File(conf.getIndexPath()), conf.getKmerSize(), conf.getScoringAlgorithmObject(), conf.getWorkerThreads());
    }
    
    private void initialize(File indexPath, int kmerSize, Similarity similarity, int workerThreads) throws Exception {
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
        
        this.workerThreads = workerThreads;
        
        // use 256MB for ram buffer
        config.setRAMBufferSizeMB(256);
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        this.indexWriter = new IndexWriter(dir, config);
        
        this.executor = Executors.newFixedThreadPool(this.workerThreads);
        
        for(int i=0;i<this.workerThreads;i++) {
            Document doc = new Document();
            Field filenameField = new StringField(IndexConstants.FIELD_FILENAME, "", Field.Store.YES);
            Field headerField = new StringField(IndexConstants.FIELD_HEADER, "", Field.Store.YES);
            Field sequenceDirectionField = new StringField(IndexConstants.FIELD_SEQUENCE_DIRECTION, "", Field.Store.YES);
            Field taxonTreeField = new StringField(IndexConstants.FIELD_TAXONOMY_TREE, "", Field.Store.YES);
            Field sequenceField = new TextField(IndexConstants.FIELD_SEQUENCE, "", Field.Store.NO);

            doc.add(filenameField);
            doc.add(headerField);
            doc.add(sequenceDirectionField);
            doc.add(taxonTreeField);
            doc.add(sequenceField);
            
            this.freeQueue.offer(doc);
        }
    }
    
    public synchronized void index(List<File> fastaDocs) throws Exception {
        if(fastaDocs == null) {
            throw new IllegalArgumentException("fastaDocs is null");
        }
        
        for(File fastaDoc : fastaDocs) {
            index(fastaDoc);
        }
    }
    
    public synchronized void index(File fastaDoc) throws Exception {
        if(fastaDoc == null) {
            throw new IllegalArgumentException("fastaDoc is null");
        }
        
        File taxonDoc = FastaFileHelper.findTaxonHierarchyDoc(fastaDoc);
        index(fastaDoc, taxonDoc);
    }
    
    public synchronized void index(File fastaDoc, File taxonDoc) throws Exception {
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
        
        while((read = reader.readNext()) != null) {
            String headerLine = read.getHeaderLine();
            if(headerLine.startsWith(">")) {
                headerLine = headerLine.substring(1);
            }
            
            final String f_filename = fastaDoc.getName();
            final String sequence = read.getSequence();
            final String header = headerLine;
            final String f_taxonTree = taxonTree;
            
            Runnable worker = new Runnable() {

                @Override
                public void run() {
                    try {
                        Document doc = freeQueue.poll();
                        if(doc == null) {
                            doc = new Document();
                            Field filenameField = new StringField(IndexConstants.FIELD_FILENAME, "", Field.Store.YES);
                            Field headerField = new StringField(IndexConstants.FIELD_HEADER, "", Field.Store.YES);
                            Field sequenceDirectionField = new StringField(IndexConstants.FIELD_SEQUENCE_DIRECTION, "", Field.Store.YES);
                            Field taxonTreeField = new StringField(IndexConstants.FIELD_TAXONOMY_TREE, "", Field.Store.YES);
                            Field sequenceField = new TextField(IndexConstants.FIELD_SEQUENCE, "", Field.Store.NO);

                            doc.add(filenameField);
                            doc.add(headerField);
                            doc.add(sequenceDirectionField);
                            doc.add(taxonTreeField);
                            doc.add(sequenceField);
                        }
                        
                        StringField filenameField = (StringField) doc.getField(IndexConstants.FIELD_FILENAME);
                        StringField headerField = (StringField) doc.getField(IndexConstants.FIELD_HEADER);
                        StringField sequenceDirectionField = (StringField) doc.getField(IndexConstants.FIELD_SEQUENCE_DIRECTION);
                        StringField taxonTreeField = (StringField) doc.getField(IndexConstants.FIELD_TAXONOMY_TREE);
                        TextField sequenceField = (TextField) doc.getField(IndexConstants.FIELD_SEQUENCE);
                        
                        filenameField.setStringValue(f_filename);
                        headerField.setStringValue(header);
                        taxonTreeField.setStringValue(f_taxonTree);
            
                        // forward-strand
                        sequenceDirectionField.setStringValue("forward");
                        sequenceField.setStringValue(sequence);
                        indexWriter.addDocument(doc);

                        // reverse-strand
                        sequenceDirectionField.setStringValue("reverse");
                        sequenceField.setStringValue(SequenceHelper.getReverseComplement(sequence));
                        indexWriter.addDocument(doc);
                        
                        freeQueue.offer(doc);
                    } catch (Exception ex) {
                        LOG.error("Exception occurred during index construction", ex);
                    }
                }
            };
            this.executor.submit(worker);
        }
        
        reader.close();
    }
    
    @Override
    public synchronized void close() throws IOException {
        try {
            this.executor.shutdown();
            this.executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            
            this.freeQueue.clear();
            
            this.analyzer.close();
            this.indexWriter.close();
        } catch (InterruptedException ex) {
            LOG.error("Interrupted", ex);
        }
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
