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

import biospectra.index.IndexConstants;
import biospectra.utils.JsonSerializer;
import java.io.File;
import java.io.IOException;
import org.apache.lucene.document.Document;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class SearchResultEntry {
    private int docId;
    private int rank;
    private String filename;
    private String header;
    private String sequenceDirection;
    private String taxonHierarchy;
    private double score;
    
    public SearchResultEntry() {
        
    }
    
    public SearchResultEntry(int docId, Document d, int rank, double score) {
        this.docId = docId;
        this.filename = d.get(IndexConstants.FIELD_FILENAME);
        this.header = d.get(IndexConstants.FIELD_HEADER);
        this.sequenceDirection = d.get(IndexConstants.FIELD_SEQUENCE_DIRECTION);
        this.taxonHierarchy = d.get(IndexConstants.FIELD_TAXONOMY_TREE);
        this.rank = rank;
        this.score = score;
    }
    
    @JsonProperty("docid")
    public int getDocId() {
        return docId;
    }

    @JsonProperty("docid")
    public void setDocId(int docId) {
        this.docId = docId;
    }

    @JsonProperty("rank")
    public int getRank() {
        return rank;
    }

    @JsonProperty("rank")
    public void setRank(int rank) {
        this.rank = rank;
    }

    @JsonProperty("filename")
    public String getFilename() {
        return filename;
    }

    @JsonProperty("filename")
    public void setFilename(String filename) {
        this.filename = filename;
    }

    @JsonProperty("header")
    public String getHeader() {
        return header;
    }

    @JsonProperty("header")
    public void setHeader(String header) {
        this.header = header;
    }
    
    @JsonProperty("sequence_direction")
    public String getSequenceDirection() {
        return sequenceDirection;
    }

    @JsonProperty("sequence_direction")
    public void setSequenceDirection(String sequenceDirection) {
        this.sequenceDirection = sequenceDirection;
    }
    
    @JsonProperty("taxon_hierarchy")
    public String getTaxonHierarchy() {
        return taxonHierarchy;
    }

    @JsonProperty("taxon_hierarchy")
    public void setTaxonHierarchy(String taxonHierarchy) {
        this.taxonHierarchy = taxonHierarchy;
    }

    @JsonProperty("score")
    public double getScore() {
        return score;
    }

    @JsonProperty("score")
    public void setScore(double score) {
        this.score = score;
    }
    
    @Override
    @JsonIgnore
    public String toString() {
        return this.rank + "\t" + this.score + "\t" + this.docId + "\t" + this.filename + "\t" + this.sequenceDirection + "\n>" + this.header + "\n" + this.taxonHierarchy;
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
