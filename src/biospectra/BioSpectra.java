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

/**
 *
 * @author iychoi
 */
public class BioSpectra {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {
        if(args[0].equalsIgnoreCase("i") || args[0].equalsIgnoreCase("index")) {
            index(args);
        } else if(args[0].equalsIgnoreCase("s") || args[0].equalsIgnoreCase("search")) {
            search(args);
        }
    }

    private static void index(String[] args) throws Exception {
        Indexer indexer = new Indexer(args[1], args[2]);
        indexer.index();
    }
    
    private static void search(String[] args) throws Exception {
        Searcher searcher = new Searcher(args[1]);
        
        searcher.search(args[2], args[3]);
    }
}
