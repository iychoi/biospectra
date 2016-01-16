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
import biospectra.classify.server.ClassificationResponseMessage;
import biospectra.ClientConfiguration;
import biospectra.classify.beans.ClassificationResult;
import biospectra.classify.server.ClassificationRequest;
import biospectra.classify.server.RabbitMQInputClient;
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
    private ArrayBlockingQueue<ClassificationRequest> requestQueue = new ArrayBlockingQueue<ClassificationRequest>(1000, true);
    private Map<Long, ClassificationRequest> requestMap = new HashMap<Long, ClassificationRequest>();
    private Map<Long, ClassificationResponseMessage> responseMap = new HashMap<Long, ClassificationResponseMessage>();
    
    public ClassifierClient(ClientConfiguration conf) throws Exception {
        if(conf == null) {
            throw new IllegalArgumentException("conf is null");
        }
        
        this.conf = conf;
        this.handler = new RabbitMQInputClient.RabbitMQInputClientEventHandler() {

            @Override
            protected void handleMessage(ClassificationResponseMessage res) {
                try {
                    returnClassificationResponse(res);
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

            ClassificationRequest creq = new ClassificationRequest();
            creq.setReqId(this.reqId);
            creq.setHeader(header);
            creq.setSequence(sequence);
        
            synchronized (this.requestQueue) {
                this.requestQueue.add(creq);
            }
            synchronized (this.requestMap) {
                this.requestMap.put(this.reqId, creq);
            
            }
            
            RabbitMQInputClient client = this.clients.get(turn);
            client.request(creq.getRequestMessage());
            // message
            LOG.info("req : " + this.reqId);
            this.reqId++;
            
            synchronized (this.requestQueue) {
                while(this.requestQueue.peek() != null && this.requestQueue.peek().getReturned()) {
                    // check returned
                    ClassificationRequest ecreq = this.requestQueue.poll();
                    synchronized (this.requestMap) {
                        this.requestMap.remove(ecreq.getReqId());
                    }

                    ClassificationResponseMessage ecres = null;
                    synchronized (this.responseMap) {
                        ecres = this.responseMap.get(ecreq.getReqId());
                        this.responseMap.remove(ecreq.getReqId());
                    }

                    ClassificationResult bresult = new ClassificationResult(ecreq.getHeader(), ecreq.getSequence(), ecres.getResult(), ecres.getType(), ecres.getTaxonRank());
                    String json = serializer.toJson(bresult);

                    summary.report(bresult);
                    bw.write(json + "\n");
                }
            }
            
            turn++;
            turn = turn % this.clients.size();
        }
        
        synchronized (this.requestQueue) {
            while(this.requestQueue.size() > 0) {
                while(this.requestQueue.peek() != null && this.requestQueue.peek().getReturned()) {
                    // check returned
                    ClassificationRequest ecreq = this.requestQueue.poll();
                    synchronized (this.requestMap) {
                        this.requestMap.remove(ecreq.getReqId());
                    }
                    
                    ClassificationResponseMessage ecres = null;
                    synchronized (this.responseMap) {
                        ecres = this.responseMap.get(ecreq.getReqId());
                        this.responseMap.remove(ecreq.getReqId());
                    }
                    
                    ClassificationResult bresult = new ClassificationResult(ecreq.getHeader(), ecreq.getSequence(), ecres.getResult(), ecres.getType(), ecres.getTaxonRank());
                    String json = serializer.toJson(bresult);

                    summary.report(bresult);
                    bw.write(json + "\n");
                }

                if(this.requestQueue.size() > 0) {
                    this.requestQueue.wait();
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
    
    private void returnClassificationResponse(ClassificationResponseMessage res) {
        // message
        LOG.info("res : " + res.getReqId());
        
        ClassificationRequest ecreq = null;
        synchronized (this.requestMap) {
            ecreq = this.requestMap.get(res.getReqId());
        }
        
        if(ecreq == null) {
            LOG.error("cannot find matching reqId = " + res.getReqId());
            return;
        }
        
        synchronized (this.responseMap) {
            this.responseMap.put(res.getReqId(), res);
        }

        ecreq.setReturned(true);
        
        synchronized (this.requestQueue) {
            this.requestQueue.notifyAll();
        }
    }

    @Override
    public void close() throws IOException {
        for(RabbitMQInputClient client : this.clients) {
            client.close();
        }
        this.clients.clear();
        this.requestQueue.clear();
        this.requestMap.clear();
        this.responseMap.clear();
    }
}
