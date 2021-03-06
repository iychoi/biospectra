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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;

/**
 *
 * @author iychoi
 */
public class KmerQueryAnalyzer extends Analyzer {

    private static final Log LOG = LogFactory.getLog(KmerQueryAnalyzer.class);
    
    private int k;
    private int skips;
    private boolean useMinStrand;

    public KmerQueryAnalyzer(int k, int skips) {
        this.k = k;
        this.skips = skips;
        this.useMinStrand = false;
    }
    
    public KmerQueryAnalyzer(int k, int skips, boolean useMinStrand) {
        this.k = k;
        this.skips = skips;
        this.useMinStrand = useMinStrand;
    }
    
    public int getK() {
        return this.k;
    }
    
    public int getSkips() {
        return this.skips;
    }
    
    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        try {
            Tokenizer tokenizer = new KmerSequenceTokenizer(this.k, this.skips);
            // use lower sequence form (forward / reverse complement)
            // use compression make 1/3 of size
            SequenceCompressFilter filter = new SequenceCompressFilter(tokenizer, true, this.useMinStrand);
            return new TokenStreamComponents(tokenizer, filter);
        } catch (IOException ex) {
            LOG.error("Exception occurred during tokenization", ex);
            return null;
        }
    }
}
