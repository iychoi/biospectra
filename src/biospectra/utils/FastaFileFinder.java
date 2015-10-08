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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 *
 * @author iychoi
 */
public class FastaFileFinder {
    public static List<File> findFastaDocs(String path) throws IOException {
        return findFastaDocs(new File(path));
    }
    
    public static List<File> findFastaDocs(File file) throws IOException {
        List<File> docs = new ArrayList<File>();
        
        if(!file.exists() || !file.canRead()) {
            throw new IOException("path " + file.getAbsolutePath() + " not exist");
        }
        
        FastaFileFilter filter = new FastaFileFilter();
        
        if(file.isFile()) {
            if(filter.accept(file)) {
                docs.add(file);
            }
        } else {
            File[] files = file.listFiles(filter);
            for(File f : files) {
                if(f.isFile()) {
                    docs.add(f);
                }
            }
        }
        
        return Collections.unmodifiableList(docs);
    }
}
