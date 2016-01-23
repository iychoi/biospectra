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
package biospectra.classify.server;

import biospectra.classify.beans.ClassificationResult;
import biospectra.classify.beans.SearchResultEntry;
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
public class ClassificationResponseMessage {
    private long reqId;
    private List<SearchResultEntry> result = new ArrayList<SearchResultEntry>();
    private ClassificationResult.ClassificationResultType type;
    private String taxonRank;
    private String taxonName;

    public static ClassificationResponseMessage createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }

        JsonSerializer serializer = new JsonSerializer();
        return (ClassificationResponseMessage) serializer.fromJsonFile(file, ClassificationResponseMessage.class);
    }
    
    public static ClassificationResponseMessage createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is empty or null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (ClassificationResponseMessage) serializer.fromJson(json, ClassificationResponseMessage.class);
    }
    
    public ClassificationResponseMessage() {
        
    }
    
    @JsonProperty("req_id")
    public void setReqId(long reqId) {
        this.reqId = reqId;
    }
    
    @JsonProperty("req_id")
    public long getReqId() {
        return reqId;
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
    public ClassificationResult.ClassificationResultType getType() {
        return type;
    }

    @JsonProperty("type")
    public void setType(ClassificationResult.ClassificationResultType type) {
        this.type = type;
    }
    
    @JsonProperty("taxon_rank")
    public void setTaxonRank(String taxonRank) {
        this.taxonRank = taxonRank;
    }
    
    @JsonProperty("taxon_rank")
    public String getTaxonRank() {
        return taxonRank;
    }
    
    @JsonProperty("taxon_name")
    public void setTaxonName(String taxonName) {
        this.taxonName = taxonName;
    }
    
    @JsonProperty("taxon_name")
    public String getTaxonName() {
        return taxonName;
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
