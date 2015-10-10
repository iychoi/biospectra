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
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class BulkSearchResultSummary {
    private long total;
    private long unknown;
    private long vague;
    private long classified;
    
    public BulkSearchResultSummary() {
        
    }
    
    @JsonProperty("total")
    public long getTotal() {
        return total;
    }

    @JsonProperty("total")
    public void setTotal(long total) {
        this.total = total;
    }

    @JsonProperty("unknown")
    public long getUnknown() {
        return unknown;
    }

    @JsonProperty("unknown")
    public void setUnknown(long unknown) {
        this.unknown = unknown;
    }
    
    @JsonIgnore
    public void increaseUnknown() {
        this.total++;
        this.unknown++;
    }

    @JsonProperty("vague")
    public long getVague() {
        return vague;
    }

    @JsonProperty("vague")
    public void setVague(long vague) {
        this.vague = vague;
    }
    
    @JsonIgnore
    public void increaseVague() {
        this.total++;
        this.vague++;
    }

    @JsonProperty("classifed")
    public long getClassified() {
        return classified;
    }

    @JsonProperty("classifed")
    public void setClassified(long classified) {
        this.classified = classified;
    }
    
    @JsonIgnore
    public void increaseClassified() {
        this.total++;
        this.classified++;
    }
    
    @JsonIgnore
    public void report(BulkSearchResult bresult) {
        this.total++;
        switch(bresult.getType()) {
            case VAGUE:
                this.vague++;
                break;
            case UNKNOWN:
                this.unknown++;
                break;
            case CLASSIFIED:
                this.classified++;
                break;
        }
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
