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
package biospectra.taxdb;

import biospectra.utils.JsonSerializer;
import java.io.File;
import java.io.IOException;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class Taxonomy {
    private int taxid;
    private String name;
    private int parent;
    private String rank;
    
    public Taxonomy() {
        
    }
    
    public Taxonomy(int taxid, String name, int parent, String rank) {
        this.taxid = taxid;
        this.name = name;
        this.parent = parent;
        this.rank = rank;
    }
    
    @JsonProperty("taxid")
    public int getTaxid() {
        return taxid;
    }

    @JsonProperty("taxid")
    public void setTaxid(int taxid) {
        this.taxid = taxid;
    }

    @JsonProperty("name")
    public String getName() {
        return name;
    }

    @JsonProperty("name")
    public void setName(String name) {
        this.name = name;
    }

    @JsonProperty("parent")
    public int getParent() {
        return parent;
    }

    @JsonProperty("parent")
    public void setParent(int parent) {
        this.parent = parent;
    }

    @JsonProperty("rank")
    public String getRank() {
        return rank;
    }

    @JsonProperty("rank")
    public void setRank(String rank) {
        this.rank = rank;
    }
    
    @Override
    @JsonIgnore
    public String toString() {
        return this.taxid + "\t" + this.name + "\t" + this.parent + "\t" + this.rank;
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
