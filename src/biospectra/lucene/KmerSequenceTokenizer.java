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
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.util.AttributeSource;

/**
 *
 * @author iychoi
 */
public class KmerSequenceTokenizer extends Tokenizer {

    public static final int DEFAULT_KMER_SIZE = 20;
    
    private int kmerSize;
    private int skips;
    private int pos;
    private int inLen; // length of the input AFTER trim()
    private int charsRead; // length of the input
    private String inStr;
    private boolean started;
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
    }
    
    @Override
    public boolean incrementToken() throws IOException {
        clearAttributes();
        
        if (!this.started) {
            this.started = true;
            char[] chars = new char[4096];
            this.charsRead = 0;
            
            while (this.charsRead < chars.length) {
                int inc = this.input.read(chars, this.charsRead, chars.length - this.charsRead);
                if (inc == -1) {
                    break;
                }
                this.charsRead += inc;
            }

            this.inStr = new String(chars, 0, this.charsRead).trim();  // remove any trailing empty strings 
            if (this.charsRead == chars.length) {
                // Read extra throwaway chars so that on end() we
                // report the correct offset:
                char[] throwaway = new char[1024];
                while (true) {
                    final int inc = this.input.read(throwaway, 0, throwaway.length);
                    if (inc == -1) {
                        break;
                    }
                    this.charsRead += inc;
                }
            }
            
            this.inLen = this.inStr.length();
            if (this.inLen == 0) {
                return false;
            }
        }
        
        if (this.pos + this.kmerSize > inLen) {            // if we hit the end of the string
            this.pos = 0;                           // reset to beginning of string
            return false;
        }
        
        int curSkip = this.skips;
        while(true) {
            boolean drop = false;
            int oldPos = this.pos;
            this.pos += 1 + curSkip;
            for(int i=oldPos;i<oldPos + this.kmerSize;i++) {
                if(this.inStr.charAt(i) != 'A' && this.inStr.charAt(i) != 'T' && 
                        this.inStr.charAt(i) != 'G' && this.inStr.charAt(i) != 'C') {
                    // wildcard found
                    drop = true;
                    curSkip = 0;
                    break;
                }
            }
            
            if(!drop) {
                this.termAtt.setEmpty().append(this.inStr, oldPos, oldPos + this.kmerSize);
                this.offsetAtt.setOffset(correctOffset(oldPos), correctOffset(oldPos + this.kmerSize));
                break;
            }
        }
        return true;
    }
    
    @Override
    public void end() {
        // set final offset
        final int finalOffset = correctOffset(this.charsRead);
        this.offsetAtt.setOffset(finalOffset, finalOffset);
    }
    
    @Override
    public void reset() throws IOException {
        super.reset();
        this.started = false;
        this.pos = 0;
    }
}
