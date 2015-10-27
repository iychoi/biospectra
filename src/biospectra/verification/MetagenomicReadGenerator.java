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
package biospectra.verification;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.yeastrc.fasta.FASTAEntry;

/**
 *
 * @author iychoi
 */
public class MetagenomicReadGenerator implements Closeable {
    private static final Log LOG = LogFactory.getLog(MetagenomicReadGenerator.class);
    
    private List<File> fastaDocs;
    private int readSize;
    private double errorRatio;
    private RandomReadSampler readSampler;
    private SequencingArtifactsSimulator artifactsSimulator;
    
    public MetagenomicReadGenerator(List<File> fastaDocs, int readSize, double errorRatio) throws Exception {
        if(fastaDocs == null) {
            throw new IllegalArgumentException("fastaDocs is null");
        }
        
        if(readSize <= 0) {
            throw new IllegalArgumentException("readSize must be larger than 0");
        }
        
        if(errorRatio < 0 || errorRatio > 1) {
            throw new IllegalArgumentException("errorRatio must be a positive value between 0 and 1");
        }
        
        initialize(fastaDocs, readSize, errorRatio);
    }
    
    private void initialize(List<File> fastaDocs, int readSize, double errorRatio) throws Exception {
        this.fastaDocs = fastaDocs;
        this.readSize = readSize;
        this.errorRatio = errorRatio;
        
        this.readSampler = new RandomReadSampler(fastaDocs, readSize);
        this.artifactsSimulator = new SequencingArtifactsSimulator();
    }
    
    public FASTAEntry generate() throws Exception {
        FASTAEntry sample = this.readSampler.sample();
        String newSequence = this.artifactsSimulator.noise(sample.getSequence(), this.errorRatio);
        
        return new FASTAEntry(sample.getHeaders(), newSequence, sample.getHeaderLine());
    }
    
    @Override
    public void close() throws IOException {
        this.fastaDocs.clear();
        this.readSize = 0;
        this.errorRatio = 0;
        this.readSampler.close();
    }
}
