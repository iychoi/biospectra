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

import biospectra.ServerConfiguration;
import biospectra.classify.beans.ClassificationResult;
import biospectra.classify.Classifier;
import biospectra.classify.server.RabbitMQInputServer.RabbitMQInputServerEventHandler;
import biospectra.utils.BlockingExecutor;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class ClassifierServer implements Closeable {
    
    private static final Log LOG = LogFactory.getLog(ClassifierServer.class);
    
    private Classifier searcher;
    private ServerConfiguration conf;
    private RabbitMQInputServer receiver;
    private RabbitMQInputServerEventHandler handler;
    private BlockingExecutor executor;
    
    public ClassifierServer(ServerConfiguration conf) throws Exception {
        if(conf == null) {
            throw new IllegalArgumentException("conf is null");
        }
        
        this.conf = conf;
        this.searcher = new Classifier(conf);
        this.handler = new RabbitMQInputServerEventHandler() {

            @Override
            protected void handleMessage(ClassificationRequestMessage req, String replyTo) {
                try {
                    addDataset(req, replyTo);
                } catch (Exception ex) {
                    LOG.error("cannot add dataset", ex);
                }
            }
            
        };
        this.receiver = new RabbitMQInputServer(conf, this.handler);
        
        this.executor = new BlockingExecutor(this.conf.getWorkerThreads(), this.conf.getWorkerThreads() * 2);
    }
    
    private synchronized void addDataset(final ClassificationRequestMessage req, final String replyTo) throws Exception {
        if(req.getSequence() == null || req.getSequence().isEmpty()) {
            throw new IllegalArgumentException("sequence is null or empty");
        }
        
        Runnable worker = new Runnable() {

            @Override
            public void run() {
                try {
                    LOG.info("classify - reqId(" + req.getReqId() + ")");
                    ClassificationResult result = searcher.classify("", req.getSequence());
                    ClassificationResponseMessage res = new ClassificationResponseMessage();
                    
                    res.setReqId(req.getReqId());
                    res.setType(result.getType());
                    res.setTaxonRank(result.getTaxonRank());
                    res.setTaxonName(result.getTaxonName());
                    res.addResult(result.getResult());
                    
                    LOG.info("return - reqId(" + req.getReqId() + ") to " + replyTo);
                    receiver.publishMessage(res, replyTo);
                } catch (Exception ex) {
                    LOG.error("Exception occurred during a classification", ex);
                }
            }
        };
        this.executor.execute(worker);
    }
    
    public void start() throws IOException {
        try {
            this.receiver.connect();
        } catch (TimeoutException ex) {
            LOG.error("Exception occurred while connecting", ex);
            throw new IOException(ex);
        }
    }
    
    @Override
    public void close() throws IOException {
        try {
            this.executor.shutdown();
            this.executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException ex) {
            LOG.error("executor service is interrupted", ex);
        }
        
        this.receiver.close();
        this.searcher.close();
    }
}
