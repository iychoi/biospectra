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
    private RabbitMQInputClient.RabbitMQInputClientEventHandler handler;
    private RabbitMQInputClient.RabbitMQInputClientEventHandler responseHandler;
    private long reqId = 0;
    
    public ClassifierClient(ClientConfiguration conf) throws Exception {
        if(conf == null) {
            throw new IllegalArgumentException("conf is null");
        }
        
        this.conf = conf;
        this.handler = new RabbitMQInputClient.RabbitMQInputClientEventHandler() {

            @Override
            public void onSuccess(long reqId, String header, String sequence, List<SearchResultEntry> result, ClassificationResult.ClassificationResultType type, String taxonRank) {
                if(responseHandler != null) {
                    responseHandler.onSuccess(reqId, header, sequence, result, type, taxonRank);
                } else {
                    LOG.error("responseHandler is not set");
                }
            }

            @Override
            public void onTimeout(long reqId, String header, String sequence) {
                if(responseHandler != null) {
                    responseHandler.onTimeout(reqId, header, sequence);
                } else {
                    LOG.error("responseHandler is not set");
                }
            }
        };
        
        for(int i=0;i<conf.getRabbitMQHostnames().size();i++) {
            RabbitMQInputClient client = new RabbitMQInputClient(conf, i, this.handler);
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
        
        int turn = 0;
        
        final JsonSerializer serializer = new JsonSerializer();
        
        this.responseHandler = new RabbitMQInputClient.RabbitMQInputClientEventHandler() {
            private int turn = 0;
            
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
                RabbitMQInputClient client = clients.get(this.turn);
                try {
                    client.request(reqId, header, sequence);
                } catch (IOException ex) {
                    LOG.error(ex);
                } catch (InterruptedException ex) {
                    LOG.error(ex);
                }
                this.turn++;
                this.turn = this.turn % clients.size();
            }
        };

        while((read = reader.readNext()) != null) {
            final String sequence = read.getSequence();
            final String header = read.getHeaderLine();

            RabbitMQInputClient client = this.clients.get(turn);
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
            
            turn++;
            turn = turn % this.clients.size();
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
