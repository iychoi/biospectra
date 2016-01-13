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
package biospectra.classify;

import biospectra.classify.beans.ClassificationResultSummary;
import biospectra.classify.server.RabbitMQInputResponse;
import biospectra.ClientConfiguration;
import biospectra.classify.beans.ClassificationResult;
import biospectra.classify.beans.SearchResultEntry;
import biospectra.classify.server.RabbitMQInputClient;
import biospectra.classify.server.RabbitMQInputRequest;
import biospectra.utils.FastaFileReader;
import biospectra.utils.JsonSerializer;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeoutException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.yeastrc.fasta.FASTAEntry;
import org.yeastrc.fasta.FASTAReader;

/**
 *
 * @author iychoi
 */
public class ClassifierClient implements Closeable {
    
    private static final Log LOG = LogFactory.getLog(ClassifierClient.class);
    
    private ClientConfiguration conf;
    private List<RabbitMQInputClient> clients = new ArrayList<RabbitMQInputClient>();
    private RabbitMQInputClient.RabbitMQInputClientEventHandler handler;
    private long reqId;
    private ArrayBlockingQueue<ClassificationResponse> waitingQueue = new ArrayBlockingQueue<ClassificationResponse>(1000, true);
    private Map<Long, ClassificationResponse> index = new HashMap<Long, ClassificationResponse>();
    
    private static class ClassificationResponse {
        private long reqId;
        private String header;
        private String sequence;
        private List<SearchResultEntry> result = new ArrayList<SearchResultEntry>();
        private ClassificationResult.ClassificationResultType type;
        private boolean classified;
        
        public ClassificationResponse() {
            
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
        
        public boolean getClassified() {
            return this.classified;
        }
        
        public void setClassified(boolean classified) {
            this.classified = classified;
        }
    }
    
    public ClassifierClient(ClientConfiguration conf) throws Exception {
        if(conf == null) {
            throw new IllegalArgumentException("conf is null");
        }
        
        this.conf = conf;
        this.handler = new RabbitMQInputClient.RabbitMQInputClientEventHandler() {

            @Override
            protected void handleMessage(RabbitMQInputResponse res) {
                try {
                    finalizeClassificationResult(res);
                } catch (Exception ex) {
                    LOG.error("cannot finalize classification result", ex);
                }
            }
        };
        
        for(int i=0;i<conf.getRabbitMQHostnames().size();i++) {
            RabbitMQInputClient client = new RabbitMQInputClient(conf, i, this.handler);
            this.clients.add(client);
        }
        
        this.reqId = 0;
    }
    
    public void start() throws IOException {
        try {
            for(RabbitMQInputClient client : this.clients) {
                client.connect();
            }
        } catch (TimeoutException ex) {
            LOG.error("Exception occurred while connecting", ex);
            throw new IOException(ex);
        }
    }
    
    public void classify(File inputFasta, File classifyOutput, File summaryOutput) throws Exception {
        if(inputFasta == null) {
            throw new IllegalArgumentException("inputFasta is null");
        }
        
        if(classifyOutput == null) {
            throw new IllegalArgumentException("classifyOutput is null");
        }
        
        if(!classifyOutput.getParentFile().exists()) {
            classifyOutput.getParentFile().mkdirs();
        }
        
        if(summaryOutput != null) {
            if(!summaryOutput.getParentFile().exists()) {
                summaryOutput.getParentFile().mkdirs();
            }
        }
        
        FASTAReader reader = FastaFileReader.getFASTAReader(inputFasta);
        FASTAEntry read = null;

        FileWriter fw = new FileWriter(classifyOutput, false);
        final BufferedWriter bw = new BufferedWriter(fw, 1024*1024);

        final ClassificationResultSummary summary = new ClassificationResultSummary();
        summary.setQueryFilename(inputFasta.getName());
        summary.setStartTime(new Date());
        
        int turn = 0;
        
        JsonSerializer serializer = new JsonSerializer();

        while((read = reader.readNext()) != null) {
            final String sequence = read.getSequence();
            final String header = read.getHeaderLine();

            ClassificationResponse cres = new ClassificationResponse();
            cres.setReqId(this.reqId);
            cres.setHeader(header);
            cres.setSequence(sequence);
            
            this.waitingQueue.add(cres);
            synchronized (this.index) {
                this.index.put(this.reqId, cres);
            }
            
            RabbitMQInputClient client = this.clients.get(turn);
            RabbitMQInputRequest req = new RabbitMQInputRequest();
            req.setReqId(this.reqId);
            req.setSequence(sequence);
            
            client.request(req);
            this.reqId++;
            
            while(this.waitingQueue.peek() != null && this.waitingQueue.peek().getClassified()) {
                // check finalized
                ClassificationResponse ecres = this.waitingQueue.poll();
                ClassificationResult bresult = new ClassificationResult(ecres.getHeader(), ecres.getSequence(), ecres.getResult(), ecres.getType());
                String json = serializer.toJson(bresult);

                summary.report(bresult);
                bw.write(json + "\n");
            }
            
            turn++;
            turn = turn % this.clients.size();
        }
        
        synchronized (this.waitingQueue) {
            while(this.waitingQueue.size() > 0) {
                while(this.waitingQueue.peek() != null && this.waitingQueue.peek().getClassified()) {
                    // check finalized
                    ClassificationResponse ecres = this.waitingQueue.poll();
                    ClassificationResult bresult = new ClassificationResult(ecres.getHeader(), ecres.getSequence(), ecres.getResult(), ecres.getType());
                    String json = serializer.toJson(bresult);

                    summary.report(bresult);
                    bw.write(json + "\n");
                }

                if(this.waitingQueue.size() > 0) {
                    this.waitingQueue.wait();
                }
            }
        }
        
        bw.close();

        summary.setEndTime(new Date());
        LOG.info("classifying of " + summary.getQueryFilename() + " finished in " + summary.getTimeTaken() + " millisec");
        
        if(summaryOutput != null) {
            summary.saveTo(summaryOutput);
        }
    }
    
    private void finalizeClassificationResult(RabbitMQInputResponse res) {
        synchronized (this.index) {
            ClassificationResponse cres = this.index.get(res.getReqId());
            if(cres == null) {
                LOG.error("cannot find matching reqId = " + res.getReqId());
            } else {
                cres.addResult(res.getResult());
                cres.setType(res.getType());
                cres.setClassified(true);
                
                this.index.remove(res.getReqId());
                synchronized (this.waitingQueue) {
                    this.waitingQueue.notifyAll();
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        for(RabbitMQInputClient client : this.clients) {
            client.close();
        }
        this.clients.clear();
        this.waitingQueue.clear();
        this.index.clear();
    }
}
