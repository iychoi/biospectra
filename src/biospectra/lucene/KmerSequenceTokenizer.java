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
package biospectra.lucene;

import java.io.IOException;
import java.io.Reader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.util.AttributeSource;

/**
 *
 * @author iychoi
 */
public class KmerSequenceTokenizer extends Tokenizer {

    private static final Log LOG = LogFactory.getLog(KmerSequenceTokenizer.class);
    
    public static final int DEFAULT_KMER_SIZE = 20;
    private static final int BUFFER_SIZE = 4096;
    
    private int kmerSize;
    private int skips;
    private char[] buffer;
    private String bufferString;
    private int inBufferOffset;
    private int bufferSize;
    private int bufferStartOffset;
    private boolean eof;
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
    
    public KmerSequenceTokenizer(Reader input, int kmerSize, int skips) {
        super(input);
        init(kmerSize, skips);
    }
    
    public KmerSequenceTokenizer(AttributeSource source, Reader input, int kmerSize, int skips) {
        super(source, input);
        init(kmerSize, skips);
    }
    
    public KmerSequenceTokenizer(AttributeFactory factory, Reader input, int kmerSize, int skips) {
        super(factory, input);
        init(kmerSize, skips);
    }
    
    public KmerSequenceTokenizer(Reader input) {
        this(input, DEFAULT_KMER_SIZE, 0);
    }
    
    private void init(int kmerSize, int skips) {
        if (kmerSize < 1) {
            throw new IllegalArgumentException("kmerSize must be greater than zero");
        }

        if (skips < 0) {
            throw new IllegalArgumentException("skips must not be greater or equal than zero");
        }

        this.kmerSize = kmerSize;
        this.skips = skips;
        this.bufferSize = 0;
        this.bufferStartOffset = 0;
        this.inBufferOffset = 0;
        this.eof = false;
    }
    
    private void bufferInput() throws IOException {
        if (!this.eof) {
            if(this.bufferSize == 0) {
                // clean start
                this.bufferStartOffset = 0;
                this.inBufferOffset = 0;
                this.buffer = new char[BUFFER_SIZE];
                int filled = 0;
                while (filled < BUFFER_SIZE) {
                    int inc = this.input.read(this.buffer, filled, BUFFER_SIZE - filled);
                    if (inc == -1) {
                        this.eof = true;
                        break;
                    }
                    filled += inc;
                }
                this.bufferSize = filled;
                this.bufferString = new String(this.buffer, 0, this.bufferSize);
            } else {
                int leftInBuffer = this.bufferSize - this.inBufferOffset;
                char[] newBuffer = new char[BUFFER_SIZE + leftInBuffer];
                // copy to new buffer
                System.arraycopy(this.buffer, this.inBufferOffset, newBuffer, 0, leftInBuffer);
                
                this.bufferStartOffset += this.inBufferOffset;
                this.inBufferOffset = 0;
                this.buffer = newBuffer;
                int filled = 0;
                while (filled < BUFFER_SIZE) {
                    int inc = this.input.read(this.buffer, leftInBuffer + filled, BUFFER_SIZE - filled);
                    if (inc == -1) {
                        this.eof = true;
                        break;
                    }
                    filled += inc;
                }
                this.bufferSize = leftInBuffer + filled;
                this.bufferString = new String(this.buffer, 0, this.bufferSize);
            }
        }
    }
    
    @Override
    public boolean incrementToken() throws IOException {
        clearAttributes();
        
        int curSkip = this.skips;
        while(true) {
            boolean drop = false;
            if (this.inBufferOffset + this.kmerSize > this.bufferSize) {
                if (this.eof) {
                    this.buffer = null;
                    this.inBufferOffset = 0;
                    this.bufferStartOffset = 0;
                    this.bufferSize = 0;
                    this.eof = false;
                    return false;
                } else {
                    bufferInput();
                }
            }
            
            for (int i = this.inBufferOffset; i < this.inBufferOffset + this.kmerSize; i++) {
                if (this.buffer[i] != 'A' && this.buffer[i] != 'T'
                        && this.buffer[i] != 'G' && this.buffer[i] != 'C') {
                    // wildcard found
                    drop = true;
                    curSkip = 0;
                    break;
                }
            }
            
            if(!drop) {
                this.termAtt.setEmpty().append(this.bufferString, this.inBufferOffset, this.inBufferOffset + this.kmerSize);
                this.offsetAtt.setOffset(this.bufferStartOffset + this.inBufferOffset, this.bufferStartOffset + this.inBufferOffset + this.kmerSize);
                this.inBufferOffset += 1 + curSkip;
                //LOG.info(this.termAtt.toString() + " : " + this.offsetAtt.startOffset() + "~" + this.offsetAtt.endOffset());
                return true;
            } else {
                this.inBufferOffset += 1 + curSkip;
            }
        }
    }
    
    @Override
    public void end() {
    }
    
    @Override
    public void reset() throws IOException {
        super.reset();
        
        this.buffer = null;
        this.inBufferOffset = 0;
        this.bufferStartOffset = 0;
        this.bufferSize = 0;
        this.eof = false;
    }
}
