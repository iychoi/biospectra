/*
 * Copyright 2016 iychoi.
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
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author iychoi
 */
public class ClassificationResponse {
    private long reqId;
    private String header;
    private String sequence;
    private List<SearchResultEntry> result = new ArrayList<SearchResultEntry>();
    private ClassificationResult.ClassificationResultType type;
    private String taxonRank;

    public ClassificationResponse() {

    }
    
    public ClassificationResponse(ClassificationRequest req, ClassificationResponseMessage resMsg) {
        this.reqId = req.getReqId();
        this.header = req.getHeader();
        this.sequence = req.getSequence();
        this.result.addAll(resMsg.getResult());
        this.type = resMsg.getType();
        this.taxonRank = resMsg.getTaxonRank();
    }

    public void setReqId(long reqId) {
        this.reqId = reqId;
    }

    public long getReqId() {
        return reqId;
    }

    public void setHeader(String header) {
        this.header = header;
    }

    public String getHeader() {
        return header;
    }

    public void setSequence(String sequence) {
        this.sequence = sequence;
    }

    public String getSequence() {
        return sequence;
    }
    
    public List<SearchResultEntry> getResult() {
        return result;
    }

    public void addResult(List<SearchResultEntry> result) {
        this.result.addAll(result);
    }

    public ClassificationResult.ClassificationResultType getType() {
        return type;
    }

    public void setType(ClassificationResult.ClassificationResultType type) {
        this.type = type;
    }
    
    public void setTaxonRank(String taxonRank) {
        this.taxonRank = taxonRank;
    }
    
    public String getTaxonRank() {
        return taxonRank;
    }
}
