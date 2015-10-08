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

import java.io.Reader;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.ngram.NGramTokenizer;
import org.apache.lucene.util.Version;

/**
 *
 * @author iychoi
 */
public class KmerAnalyzer extends Analyzer {
    private int k;

    public KmerAnalyzer(int k) {
        this.k = k;
    }
    
    @Override
    protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer tokenizer = new NGramTokenizer(reader, this.k, this.k);
        LowerCaseFilter filter = new LowerCaseFilter(Version.LUCENE_40, tokenizer);
        return new TokenStreamComponents(tokenizer, filter);
    }
}
