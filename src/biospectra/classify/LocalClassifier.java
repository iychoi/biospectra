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
import biospectra.classify.beans.ClassificationResultSummary;
import biospectra.Configuration;
import biospectra.utils.BlockingExecutor;
import biospectra.utils.FastaFileReader;
import biospectra.utils.JsonSerializer;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.yeastrc.fasta.FASTAEntry;
import org.yeastrc.fasta.FASTAReader;

/**
 *
 * @author iychoi
 */
public class LocalClassifier implements Closeable {
    
    private static final Log LOG = LogFactory.getLog(LocalClassifier.class);
    
    private Classifier classifier;
    private Configuration conf;
    
    public LocalClassifier(Configuration conf) throws Exception {
        this.conf = conf;
        this.classifier = new Classifier(conf);
    }
    
    public ClassificationResult classify(String header, String sequence) throws Exception {
        if(sequence == null || sequence.isEmpty()) {
            throw new IllegalArgumentException("sequence is null or empty");
        }
        
        return this.classifier.classify(header, sequence);
    }
    
    public void classify(File inputFasta, File classifyOutput, File summaryOutput) throws Exception {
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

        final ClassificationResultSummary summary = new ClassificationResultSummary();
        summary.setQueryFilename(inputFasta.getName());
        summary.setStartTime(new Date());

        int threads = this.conf.getWorkerThreads();
        BlockingExecutor executor = new BlockingExecutor(threads, threads * 2);
        
        while((read = reader.readNext()) != null) {
            final String sequence = read.getSequence();
            final String header = read.getHeaderLine();

            Runnable worker = new Runnable() {

                @Override
                public void run() {
                    try {
                        ClassificationResult result = classifier.classify(header, sequence);
                        
                        JsonSerializer serializer = new JsonSerializer();
                        String json = serializer.toJson(result);
                        
                        synchronized(summary) {
                            summary.report(result);
                        }
                        synchronized(bw) {
                            bw.write(json + "\n");
                        }
                    } catch (Exception ex) {
                        LOG.error("Exception occurred during search", ex);
                    }
                }
            };
            executor.execute(worker);
        }
        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

        bw.close();
        reader.close();

        summary.setEndTime(new Date());
        LOG.info("classifying " + summary.getQueryFilename() + " finished in " + summary.getTimeTaken() + " millisec");
        
        if(summaryOutput != null) {
            summary.saveTo(summaryOutput);
        }
    }

    @Override
    public void close() throws IOException {
        this.classifier.close();
    }
}
