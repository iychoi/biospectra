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
import java.util.ArrayList;
import java.util.List;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class BulkSearchResult {

    public enum SearchResultType {
        UNKNOWN,
        VAGUE,
        CLASSIFIED,
    }

    private String query;
    private List<SearchResult> result = new ArrayList<SearchResult>();
    private SearchResultType type;
            
    public BulkSearchResult(String query, List<SearchResult> result) {
        this.query = query;
        if(result != null) {
            this.result.addAll(result);
            this.type = determineType(result);
        } else {
            this.type = SearchResultType.UNKNOWN;
        }
    }
    
    @JsonIgnore
    private SearchResultType determineType(List<SearchResult> result) {
        if(result.isEmpty()) {
            return SearchResultType.UNKNOWN;
        } else if(result.size() == 1) {
            return SearchResultType.CLASSIFIED;
        } else {
            List<SearchResult> top = new ArrayList<SearchResult>();
            double score_top = result.get(0).getScore();
            for(SearchResult r : result) {
                if(r.getScore() == score_top) {
                    top.add(r);
                }
            }
            
            if(top.size() > 1) {
                return SearchResultType.VAGUE;
            } else {
                return SearchResultType.CLASSIFIED;
            }
        }
    }
    
    @JsonProperty("query")
    public String getQuery() {
        return query;
    }

    @JsonProperty("query")
    public void setQuery(String query) {
        this.query = query;
    }

    @JsonProperty("result")
    public List<SearchResult> getResult() {
        return result;
    }

    @JsonProperty("result")
    public void addResult(List<SearchResult> result) {
        this.result.addAll(result);
    }

    @JsonProperty("type")
    public SearchResultType getType() {
        return type;
    }

    @JsonProperty("type")
    public void setType(SearchResultType type) {
        this.type = type;
    }
    
    @Override
    @JsonIgnore
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for(SearchResult sr: this.result) {
            sb.append(sr.toString());
            sb.append("\n");
        }
        
        return this.query + "\t" + this.type.name() + "\n" + sb.toString();
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
