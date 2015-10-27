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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 *
 * @author iychoi
 */
public class SequencingArtifactsSimulator {
    private Random random;
    private char[][] errorTable = {{'C', 'G', 'T'}, 
        {'A', 'G', 'T'},
        {'A', 'C', 'T'},
        {'A', 'C', 'G'},
    };
    
    public SequencingArtifactsSimulator() {
        this.random = new Random();
    }
    
    public String noise(String sequence, double noiseRatio) {
        if(sequence == null || sequence.isEmpty()) {
            throw new IllegalArgumentException("sequence is null or empty");
        }
        
        if(noiseRatio < 0 || noiseRatio > 1) {
            throw new IllegalArgumentException("noiseRatio must be a positive value between 0 and 1");
        }
        
        int numError = (int) (sequence.length() * noiseRatio);
        List<Integer> choiceArray = new ArrayList<Integer>();
        for(int i=0;i<sequence.length();i++) {
            choiceArray.add(i);
        }
        
        List<Integer> posArray = new ArrayList<Integer>();
        while(posArray.size() < numError) {
            int pos = this.random.nextInt(choiceArray.size());
            posArray.add(choiceArray.get(pos));
            choiceArray.remove(pos);
        }
        
        char[] newSequence = sequence.toCharArray();
        for(int pos : posArray) {
            int noise = this.random.nextInt(3);
            char noiseChar = 'A';
            switch(sequence.charAt(pos)) {
                case 'A':
                    noiseChar = this.errorTable[0][noise];
                    break;
                case 'C':
                    noiseChar = this.errorTable[1][noise];
                    break;
                case 'G':
                    noiseChar = this.errorTable[2][noise];
                    break;
                case 'T':
                    noiseChar = this.errorTable[3][noise];
                    break;
            }
            newSequence[pos] = noiseChar;
        }
        
        return new String(newSequence);
    }
}
