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

import biospectra.utils.JsonSerializer;
import java.io.File;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class Configuration {
    private static final Log LOG = LogFactory.getLog(Configuration.class);
    
    public static final int DEFAULT_KMERSIZE = 20;
    public static final int DEFAULT_QUERY_TERM_SKIPS = 10;
    public static final double DEFAULT_QUERY_TERMS_MIN_SHOULD_MATCH = 0.7;

    private String indexPath;
    private int kmerSize = DEFAULT_KMERSIZE;
    private int queryTermSkips = DEFAULT_QUERY_TERM_SKIPS;
    private double queryMinShouldMatch = DEFAULT_QUERY_TERMS_MIN_SHOULD_MATCH;

    public static Configuration createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }

        JsonSerializer serializer = new JsonSerializer();
        return (Configuration) serializer.fromJsonFile(file, Configuration.class);
    }
    
    public static Configuration createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is empty or null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (Configuration) serializer.fromJson(json, Configuration.class);
    }
    
    public Configuration() {
        
    }

    @JsonProperty("index_path")
    public String getIndexPath() {
        return indexPath;
    }

    @JsonProperty("index_path")
    public void setIndexPath(String indexPath) {
        this.indexPath = indexPath;
    }
    
    @JsonProperty("kmer_size")
    public int getKmerSize() {
        return kmerSize;
    }

    @JsonProperty("kmer_size")
    public void setKmerSize(int kmerSize) {
        this.kmerSize = kmerSize;
    }
    
    @JsonProperty("query_term_skips")
    public int getQueryTermSkips() {
        return queryTermSkips;
    }

    @JsonProperty("query_term_skips")
    public void setQueryTermSkips(int queryTermSkips) {
        this.queryTermSkips = queryTermSkips;
    }
    
    @JsonProperty("query_term_min_should_match")
    public double getQueryMinShouldMatch() {
        return queryMinShouldMatch;
    }

    @JsonProperty("query_term_min_should_match")
    public void setQueryMinShouldMatch(double queryMinShouldMatch) {
        this.queryMinShouldMatch = queryMinShouldMatch;
    }
    
    @JsonIgnore
    public synchronized String toJson() throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return serializer.toJson(this);
    }
    
    @JsonIgnore
    public synchronized void saveTo(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        serializer.toJsonFile(file, this);
    }
}
