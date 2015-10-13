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

import java.io.Reader;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;

/**
 *
 * @author iychoi
 */
public class KmerQueryAnalyzer extends Analyzer {
    private int k;
    private int skips;

    public KmerQueryAnalyzer(int k, int skips) {
        this.k = k;
        this.skips = skips;
    }
    
    @Override
    protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer tokenizer = new KmerTokenizer(reader, this.k, this.skips);
        // use lower sequence form (forward / reverse complement)
        // use compression make 1/3 of size
        LowerSequenceFormFilter filter = new LowerSequenceFormFilter(tokenizer, true);
        return new TokenStreamComponents(tokenizer, filter);
    }
}
