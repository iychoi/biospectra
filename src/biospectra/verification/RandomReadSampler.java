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

import biospectra.utils.FastaFileReader;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.yeastrc.fasta.FASTAEntry;
import org.yeastrc.fasta.FASTAReader;

/**
 *
 * @author iychoi
 */
public class RandomReadSampler implements Closeable {

    private static final Log LOG = LogFactory.getLog(RandomReadSampler.class);
    
    private List<File> fastaDocs;
    private int readSize;
    private List<ReadInfo> readInfo;
    private long rangeMax;
    private Random random;
    
    public RandomReadSampler(List<File> fastaDocs, int readSize) throws Exception {
        if(fastaDocs == null) {
            throw new IllegalArgumentException("fastaDocs is null");
        }
        
        if(readSize <= 0) {
            throw new IllegalArgumentException("readSize must be larger than 0");
        }
        
        initialize(fastaDocs, readSize);
    }
    
    private void initialize(List<File> fastaDocs, int readSize) throws Exception {
        this.fastaDocs = fastaDocs;
        this.readSize = readSize;
        
        // precompute readinfo
        this.readInfo = makeReadInfo(fastaDocs);
        this.rangeMax = 0;
        for(ReadInfo ri : this.readInfo) {
            this.rangeMax += ri.getSize();
        }
        
        this.random = new Random();
    }
    
    private List<ReadInfo> makeReadInfo(List<File> fastaDocs) throws Exception {
        List<ReadInfo> readInfoArr = new ArrayList<ReadInfo>();
        for(File fastaDoc : fastaDocs) {
            readInfoArr.addAll(makeReadInfo(fastaDoc));
        }
        return readInfoArr;
    }
    
    private List<ReadInfo> makeReadInfo(File fastaDoc) throws Exception {
        List<ReadInfo> readInfoArr = new ArrayList<ReadInfo>();
        FASTAReader reader = FastaFileReader.getFASTAReader(fastaDoc);
        FASTAEntry read = null;
        
        int id = 0;
        while((read = reader.readNext()) != null) {
            String sequence = read.getSequence();
            ReadInfo info = new ReadInfo(fastaDoc, id, sequence.length());
            readInfoArr.add(info);
            id++;
        }
        
        reader.close();
        return readInfoArr;
    }
    
    private FASTAEntry trySample(int readSize) throws Exception {
        long random = Math.abs(this.random.nextLong()) % this.rangeMax;
        long left = random;
        for(ReadInfo ri : this.readInfo) {
            if(left <= ri.getSize()) {
                if(ri.getSize() - left >= readSize) {
                    // go for it
                    FASTAReader reader = FastaFileReader.getFASTAReader(ri.getFastaFile());
                    FASTAEntry read = null;
                    int id = 0;
                    while((read = reader.readNext()) != null) {
                        if(id == ri.getId()) {
                            String sequence = read.getSequence().substring((int) left, (int) (left + readSize));
                            FASTAEntry entry = new FASTAEntry(read.getHeaders(), sequence, read.getHeaderLine());
                            return entry;
                        }
                        id++;
                    }
                    return null;
                } else {
                    return null;
                }
            } else {
                left -= ri.getSize();
                
                if(left <= 0) {
                    break;
                }
            }
        }
        return null;
    }
    
    public FASTAEntry sample() throws Exception {
        FASTAEntry entry = null;
        int variant = this.random.nextInt(this.readSize / 5) - (this.readSize / 10);
        int newReadSize = this.readSize + variant;
        if(newReadSize <= 0) {
            newReadSize = this.readSize;
        }
        
        do {
            entry = trySample(newReadSize);
        } while(entry == null);
        
        return entry;
    }
    
    @Override
    public void close() throws IOException {
        this.fastaDocs.clear();
        this.readSize = 0;
        this.readInfo.clear();
    }
}
