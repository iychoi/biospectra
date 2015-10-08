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

import biospectra.utils.SequenceHelper;
import java.io.IOException;
import org.apache.commons.codec.binary.Base64;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

/**
 *
 * @author iychoi
 */
public final class LowerSequenceFormFilter extends TokenFilter {

    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private boolean compress = true;
    
    public LowerSequenceFormFilter(TokenStream in) {
        super(in);
    }
    
    public LowerSequenceFormFilter(TokenStream in, boolean compress) {
        super(in);
        
        this.compress = compress;
    }
    
    @Override
    public boolean incrementToken() throws IOException {
        if (this.input.incrementToken()) {
            char[] buffer = this.termAtt.buffer();
            final int length = this.termAtt.length();
            
            char[] reverse_complement = SequenceHelper.getReverseComplement(buffer, length);
            boolean useReverse = false;
            for(int i=0;i<length;i++) {
                if(buffer[i] > reverse_complement[i]) {
                    useReverse = true;
                    break;
                } else if(buffer[i] < reverse_complement[i]) {
                    useReverse = false;
                    break;
                }
                
                // otherwise, loop
            }
            
            //System.out.println("ori: " + String.copyValueOf(buffer));
            
            if(useReverse) {
                for(int i=0;i<length;i++) {
                    buffer[i] = reverse_complement[i];
                }
            }
            
            //System.out.println("new: " + String.copyValueOf(buffer));
            
            if(this.compress) {
                byte[] compressed = SequenceHelper.compress(buffer, length);
                byte[] encoded = Base64.encodeBase64(compressed);

                buffer = this.termAtt.resizeBuffer(encoded.length);

                for(int i=0;i<buffer.length;i++) {
                    if(i < encoded.length) {
                        buffer[i] = (char)encoded[i];
                    } else {
                        buffer[i] = 0;
                    }
                }

                this.termAtt.setLength(encoded.length);
                //System.out.println("comp: " + String.copyValueOf(buffer));
            }
            return true;
        } else {
            return false;
        }
    }
    
}
