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
package biospectra.utils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.zip.GZIPInputStream;
import org.yeastrc.fasta.FASTAReader;

/**
 *
 * @author iychoi
 */
public class FastaFileReader {
    public static FASTAReader getFASTAReader(File fastaDoc) throws Exception {
        CompressedFileFilter filter = new CompressedFileFilter();
        
        if(filter.accept(fastaDoc)) {
            // compressed
            return FASTAReader.getInstance(new BufferedInputStream(new GZIPInputStream(new FileInputStream(fastaDoc))));
        } else {
            // plain
            return FASTAReader.getInstance(fastaDoc);
        }
    }
}
