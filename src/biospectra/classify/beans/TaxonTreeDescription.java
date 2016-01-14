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
public class TaxonTreeDescription {
    private List<Taxonomy> tree = new ArrayList<Taxonomy>();
    
    public static TaxonTreeDescription createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is empty or null");
        }
        
        if(json.startsWith("[") && json.endsWith("]")) {
            JsonSerializer serializer = new JsonSerializer();
            return (TaxonTreeDescription) serializer.fromJson("{\"tree\"=" + json + "}", TaxonTreeDescription.class);
        } else if(json.startsWith("{") && json.endsWith("}")) {
            JsonSerializer serializer = new JsonSerializer();
            return (TaxonTreeDescription) serializer.fromJson(json, TaxonTreeDescription.class);
        } else {
            // not an object
            throw new IOException("cannot create instance from wrong json format");
        }
    }
    
    public TaxonTreeDescription() {
        
    }
    
    @JsonProperty("tree")
    public void addTaxonomyTree(List<Taxonomy> tree) {
        this.tree.addAll(tree);
    }
    
    @JsonIgnore
    public void addTaxonomy(Taxonomy tax) {
        this.tree.add(tax);
    }
    
    @JsonProperty("tree")
    public List<Taxonomy> getTaxonomyTree() {
        return this.tree;
    }

    @JsonIgnore
    public String getLowestRank() {
        if(this.tree.isEmpty()) {
            return "";
        } else {
            for(Taxonomy tax : this.tree) {
                String rank = tax.getRank();
                if(rank != null && !rank.isEmpty() && !rank.equalsIgnoreCase("no rank")) {
                    return rank;
                }
            }
            return "";
        }
    }
    
    @JsonIgnore
    public String getLowestRank(int idx) {
        if(idx <= 0) {
            return getLowestRank();
        }
        
        if(this.tree.isEmpty()) {
            return "";
        } else {
            for(int i=idx;i<this.tree.size();i++) {
                Taxonomy tax = this.tree.get(i);
                String rank = tax.getRank();
                if(rank != null && !rank.isEmpty() && !rank.equalsIgnoreCase("no rank")) {
                    return rank;
                }
            }
            return "";
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
