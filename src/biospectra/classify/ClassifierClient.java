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
import biospectra.ClientConfiguration;
import biospectra.classify.beans.ClassificationResult;
import biospectra.classify.beans.SearchResultEntry;
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
import java.util.List;
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
    private ClientEventHandler responseHandler;
    private long reqId = 0;
    
    public static abstract class ClientEventHandler {
        public abstract void onSuccess(long reqId, String header, String sequence, List<SearchResultEntry> result, ClassificationResult.ClassificationResultType type, String taxonRank);
        public abstract void onTimeout(long reqId, String header, String sequence);
    }
    
    public ClassifierClient(ClientConfiguration conf) throws Exception {
        if(conf == null) {
            throw new IllegalArgumentException("conf is null");
        }
        
        this.conf = conf;
        
        for(int i=0;i<conf.getRabbitMQHostnames().size();i++) {
            
            RabbitMQInputClient.RabbitMQInputClientEventHandler handler = new RabbitMQInputClient.RabbitMQInputClientEventHandler() {
                @Override
                public void onSuccess(long reqId, String header, String sequence, List<SearchResultEntry> result, ClassificationResult.ClassificationResultType type, String taxonRank) {
                    if(responseHandler != null) {
                        synchronized (responseHandler) {
                            responseHandler.onSuccess(reqId, header, sequence, result, type, taxonRank);
                        }
                    } else {
                        LOG.error("responseHandler is not set");
                    }
                }

                @Override
                public void onTimeout(long reqId, String header, String sequence) {
                    if(responseHandler != null) {
                        synchronized (responseHandler) {
                            responseHandler.onTimeout(reqId, header, sequence);
                            this.getClient().setReachable(false);
                        }
                    } else {
                        LOG.error("responseHandler is not set");
                    }
                }
            };
            
            RabbitMQInputClient client = new RabbitMQInputClient(conf, i, handler);
            this.clients.add(client);
        }
    }
    
    public synchronized void start() throws IOException {
        try {
            for(RabbitMQInputClient client : this.clients) {
                client.connect();
            }
        } catch (TimeoutException ex) {
            LOG.error("Exception occurred while connecting", ex);
            throw new IOException(ex);
        }
        
        this.reqId = 0;
    }
    
    private synchronized RabbitMQInputClient getNextLiveClient() {
        int liveClients = 0;
        for(RabbitMQInputClient client : this.clients) {
            if(client.isReachable()) {
                liveClients++;
            }
        }
        
        if(liveClients <= 0) {
            return null;
        }
        
        int turn = (int) (this.reqId % liveClients);
        int client_passed = 0;
        for(RabbitMQInputClient client : this.clients) {
            if(client.isReachable()) {
                if(client_passed == turn) {
                    return client;
                } else {
                    client_passed++;
                }
            }
        }
        return null;
    }
    
    public synchronized void classify(File inputFasta, File classifyOutput, File summaryOutput) throws Exception {
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
        
        final JsonSerializer serializer = new JsonSerializer();
        
        this.responseHandler = new ClientEventHandler() {
            
            @Override
            public void onSuccess(long reqId, String header, String sequence, List<SearchResultEntry> result, ClassificationResult.ClassificationResultType type, String taxonRank) {
                ClassificationResult bresult = new ClassificationResult(header, sequence, result, type, taxonRank);
                String json;
                try {
                    json = serializer.toJson(bresult);
                    summary.report(bresult);
                    bw.write(json + "\n");
                } catch (IOException ex) {
                    LOG.error("Cannot write to a file", ex);
                }
            }

            @Override
            public void onTimeout(long reqId, String header, String sequence) {
                RabbitMQInputClient client = getNextLiveClient();
                if(client == null) {
                    LOG.error("no live client");
                    return;
                }
                
                try {
                    client.request(reqId, header, sequence);
                } catch (IOException ex) {
                    LOG.error(ex);
                } catch (InterruptedException ex) {
                    LOG.error(ex);
                }
            }
        };

        while((read = reader.readNext()) != null) {
            final String sequence = read.getSequence();
            final String header = read.getHeaderLine();

            RabbitMQInputClient client = getNextLiveClient();
            if(client == null) {
                LOG.error("no live client");
                return;
            }
            
            if(client.requestWouldBlock()) {
                try {
                    client.waitForTasks();
                } catch (InterruptedException ex) {
                    LOG.error(ex);
                }
            }

            try {
                client.request(this.reqId, header, sequence);
            } catch (IOException ex) {
                LOG.error(ex);
            } catch (InterruptedException ex) {
                LOG.error(ex);
            }
            this.reqId++;
        }
        
        for(RabbitMQInputClient client : this.clients) {
            client.waitForTasks();
        }
        
        bw.close();
        
        this.responseHandler = null;

        summary.setEndTime(new Date());
        LOG.info("classifying of " + summary.getQueryFilename() + " finished in " + summary.getTimeTaken() + " millisec");
        
        if(summaryOutput != null) {
            summary.saveTo(summaryOutput);
        }
    }    

    @Override
    public synchronized void close() throws IOException {
        for(RabbitMQInputClient client : this.clients) {
            client.close();
        }
        this.clients.clear();
    }
}
