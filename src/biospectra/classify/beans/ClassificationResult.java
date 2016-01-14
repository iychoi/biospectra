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
package biospectra.classify.beans;

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
public class ClassificationResult {
    public enum ClassificationResultType {
        UNKNOWN,
        VAGUE,
        CLASSIFIED,
    }
    
    private String queryHeader;
    private String query;
    private List<SearchResultEntry> result = new ArrayList<SearchResultEntry>();
    private ClassificationResultType type;
    private String taxonRank;
            
    public ClassificationResult(String queryHeader, String query, List<SearchResultEntry> result, ClassificationResultType type, String taxonRank) {
        initialize(queryHeader, query, result, type, taxonRank);
    }
    
    private void initialize(String queryHeader, String query, List<SearchResultEntry> result, ClassificationResultType type, String taxonRank) {
        this.queryHeader = queryHeader;
        this.query = query;
        if(result != null) {
            this.result.addAll(result);
        }
        this.type = type;
        this.taxonRank = taxonRank;
    }
    
    @JsonProperty("query_header")
    public String getQueryHeader() {
        return queryHeader;
    }

    @JsonProperty("query_header")
    public void setQueryHeader(String queryHeader) {
        this.queryHeader = queryHeader;
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
    public List<SearchResultEntry> getResult() {
        return result;
    }

    @JsonProperty("result")
    public void addResult(List<SearchResultEntry> result) {
        this.result.addAll(result);
    }

    @JsonProperty("type")
    public ClassificationResultType getType() {
        return type;
    }

    @JsonProperty("type")
    public void setType(ClassificationResultType type) {
        this.type = type;
    }
    
    @JsonProperty("taxon_rank")
    public String getTaxonRank() {
        return taxonRank;
    }

    @JsonProperty("taxon_rank")
    public void setTaxonRank(String taxonRank) {
        this.taxonRank = taxonRank;
    }
    
    @Override
    @JsonIgnore
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for(SearchResultEntry sr: this.result) {
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
