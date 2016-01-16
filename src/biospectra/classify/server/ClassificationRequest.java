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

import biospectra.classify.beans.SearchResultEntry;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author iychoi
 */
public class ClassificationRequest {
    private long reqId;
    private String header;
    private String sequence;
    private List<SearchResultEntry> result = new ArrayList<SearchResultEntry>();
    private boolean returned;

    public ClassificationRequest() {

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

    public synchronized boolean getReturned() {
        return this.returned;
    }

    public synchronized void setReturned(boolean returned) {
        this.returned = returned;
    }
    
    public ClassificationRequestMessage getRequestMessage() {
        ClassificationRequestMessage reqMsg = new ClassificationRequestMessage();
        reqMsg.setReqId(this.reqId);
        reqMsg.setSequence(this.sequence);
        return reqMsg;
    }
}
