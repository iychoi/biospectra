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
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class Configuration {
    private static final Log LOG = LogFactory.getLog(Configuration.class);
    
    public static final int DEFAULT_KMERSIZE = 10;
    public static final int DEFAULT_KMERSKIPS = 5;
    public static final double DEFAULT_QUERY_TERMS_MIN_SHOULD_MATCH = 0.5;
    public static final int DEFAULT_WORKER_THREADS = 4;
    public static final String DEFAULT_SCORING_ALGORITHM = "default";
    
    private String indexPath;
    private int kmerSize = DEFAULT_KMERSIZE;
    private int kmerSkips = DEFAULT_KMERSKIPS;
    private double queryMinShouldMatch = DEFAULT_QUERY_TERMS_MIN_SHOULD_MATCH;
    private int workerThreads = DEFAULT_WORKER_THREADS;
    private String scoringAlgorithm = DEFAULT_SCORING_ALGORITHM;

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
    
    @JsonProperty("kmer_skips")
    public int getKmerSkips() {
        return kmerSkips;
    }
    
    @JsonProperty("kmer_skips")
    public void setKmerSkips(int kmerSkips) {
        this.kmerSkips = kmerSkips;
    }
    
    @JsonProperty("query_term_min_should_match")
    public double getQueryMinShouldMatch() {
        return queryMinShouldMatch;
    }

    @JsonProperty("query_term_min_should_match")
    public void setQueryMinShouldMatch(double queryMinShouldMatch) {
        this.queryMinShouldMatch = queryMinShouldMatch;
    }
    
    @JsonProperty("worker_threads")
    public int getWorkerThreads() {
        return workerThreads;
    }

    @JsonProperty("worker_threads")
    public void setWorkerThreads(int workerThreads) {
        this.workerThreads = workerThreads;
    }
    
    @JsonProperty("scoring_algorithm")
    public String getScoringAlgorithm() {
        return scoringAlgorithm;
    }
    
    @JsonProperty("scoring_algorithm")
    public void setScoringAlgorithm(String scoringAlgorithm) {
        this.scoringAlgorithm = scoringAlgorithm;
    }
    
    @JsonIgnore
    public Similarity getScoringAlgorithmObject() {
        if(this.scoringAlgorithm == null || this.scoringAlgorithm.isEmpty() || this.scoringAlgorithm.equals(DEFAULT_SCORING_ALGORITHM) || this.scoringAlgorithm.equalsIgnoreCase("tfidf") || this.scoringAlgorithm.equalsIgnoreCase("vectorspace")) {
            // vector-space model
            return new DefaultSimilarity();
        } else if(this.scoringAlgorithm.equalsIgnoreCase("bm25")) {
            // bm25 probability model
            return new BM25Similarity();
        }
        
        return new DefaultSimilarity();
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
