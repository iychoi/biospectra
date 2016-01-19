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

/**
 *
 * @author iychoi
 */
public class ClassificationRequest {
    private long reqId;
    private String header;
    private String sequence;
    private long sentTime;
    private RequestStatus status = RequestStatus.STATUS_UNKNOWN;
    
    public static enum RequestStatus {
        STATUS_UNKNOWN,
        STATUS_RESPONDED,
        STATUS_TIMEOUT,
    }
    
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
    
    public synchronized void setSentTime(long sentTime) {
        this.sentTime = sentTime;
    }
    
    public synchronized long getSentTime() {
        return this.sentTime;
    }
    
    public synchronized void setStatus(RequestStatus status) {
        this.status = status;
    }
    
    public synchronized RequestStatus getStatus() {
        return this.status;
    }
    
    public ClassificationRequestMessage getRequestMessage() {
        ClassificationRequestMessage reqMsg = new ClassificationRequestMessage();
        reqMsg.setReqId(this.reqId);
        reqMsg.setSequence(this.sequence);
        return reqMsg;
    }
    
    @Override
    public int hashCode() {
        int hash = 7;
        hash = 73 * hash + (int) (this.reqId ^ (this.reqId >>> 32));
        return hash;
    }
    
    @Override
    public boolean equals(Object o) {
        if(o instanceof ClassificationRequest && ((ClassificationRequest)o).reqId == this.reqId) {
            return true;
        } else {
            return false;
        }
    }
}
