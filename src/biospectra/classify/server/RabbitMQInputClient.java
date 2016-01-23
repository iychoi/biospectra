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

import biospectra.ClientConfiguration;
import biospectra.classify.beans.ClassificationResult;
import biospectra.classify.beans.SearchResultEntry;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import java.io.Closeable;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeoutException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class RabbitMQInputClient implements Closeable {
    
    private static final Log LOG = LogFactory.getLog(RabbitMQInputClient.class);
    
    private static final int QUEUE_SIZE = 4;
    private static final long QUERY_TIMEOUT = 300*1000;
    private static final long TIMEOUT_MAX_COUNT = 3;
    
    private ClientConfiguration conf;
    private int hostId;
    private RabbitMQInputClientEventHandler handler;
    private Connection connection;
    private Channel requestChannel;
    private Channel responseChannel;
    private Consumer consumer;
    private Thread workerThread;
    private String queueName;
    private boolean reachable = false;
    private ArrayBlockingQueue<ClassificationRequest> requestQueue = new ArrayBlockingQueue<ClassificationRequest>(QUEUE_SIZE, true);
    private Map<Long, ClassificationRequest> requestMap = new HashMap<Long, ClassificationRequest>();
    private Thread timeoutThread;
    private int timeout;

    public static abstract class RabbitMQInputClientEventHandler {
        private RabbitMQInputClient client;
        
        public void setClient(RabbitMQInputClient client) {
            this.client = client;
        }
        
        public RabbitMQInputClient getClient() {
            return this.client;
        }
        
        public abstract void onSuccess(long reqId, String header, String sequence, List<SearchResultEntry> result, ClassificationResult.ClassificationResultType type, String taxonRank, String taxonName);
        public abstract void onTimeout(long reqId, String header, String sequence);
    }
    
    public RabbitMQInputClient(ClientConfiguration conf, int hostId, RabbitMQInputClientEventHandler handler) {
        if(conf == null) {
            throw new IllegalArgumentException("conf is null");
        }
        
        if(hostId < 0 || hostId >= conf.getRabbitMQHostnames().size()) {
            throw new IllegalArgumentException("hostId is invalid");
        }
        
        if(handler == null) {
            throw new IllegalArgumentException("handler is null");
        }
        
        this.conf = conf;
        this.hostId = hostId;
        this.handler = handler;
        this.handler.setClient(this);
    }
    
    public synchronized void connect() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        String hostname = this.conf.getRabbitMQHostnames().get(this.hostId);
        factory.setHost(hostname);
        factory.setPort(this.conf.getRabbitMQPort());
        factory.setUsername(this.conf.getRabbitMQUserId());
        factory.setPassword(this.conf.getRabbitMQUserPwd());
        
        factory.setAutomaticRecoveryEnabled(true);
        
        this.connection = factory.newConnection();
        this.connection.addShutdownListener(new ShutdownListener(){

            @Override
            public void shutdownCompleted(ShutdownSignalException sse) {
                LOG.error("connection shutdown", sse);
            }
        });
        
        this.requestChannel = this.connection.createChannel();
        this.responseChannel = this.connection.createChannel();
        
        LOG.info("reader connected - " + hostname + ":" + this.conf.getRabbitMQPort());
        
        this.responseChannel.basicQos(10);
        this.queueName = this.responseChannel.queueDeclare().getQueue();
        
        this.consumer = new DefaultConsumer(this.responseChannel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, 
                    AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");

                this.getChannel().basicAck(envelope.getDeliveryTag(), false);
                
                LOG.info("> " + message);

                ClassificationResponseMessage res = ClassificationResponseMessage.createInstance(message);
                ClassificationRequest ereq = null;
                synchronized (requestMap) {
                    ereq = requestMap.get(res.getReqId());
                    if(ereq != null) {
                        requestMap.remove(res.getReqId());
                    }
                }

                if(ereq == null) {
                    LOG.error("cannot find matching request");
                } else {
                    ClassificationResponse eres = new ClassificationResponse(ereq, res);

                    boolean responded = false;
                    synchronized (ereq) {
                        ClassificationRequest.RequestStatus status = ereq.getStatus();
                        if(status.equals(ClassificationRequest.RequestStatus.STATUS_UNKNOWN)) {
                            ereq.setStatus(ClassificationRequest.RequestStatus.STATUS_RESPONDED);
                            responded = true;
                        }

                        requestQueue.remove(ereq);
                    }

                    if(responded) {
                        LOG.info("res : " + ereq.getReqId());
                        if(handler != null) {
                            handler.onSuccess(eres.getReqId(), eres.getHeader(), eres.getSequence(), eres.getResult(), eres.getType(), eres.getTaxonRank(), eres.getTaxonName());
                        }

                        synchronized (requestQueue) {
                            requestQueue.notifyAll();
                        }
                    }
                }
            }
        };
        
        this.workerThread = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    responseChannel.basicConsume(queueName, consumer);
                    LOG.info("Waiting for messages");
                } catch (IOException ex) {
                    LOG.error("Exception occurred while consuming a message", ex);
                }
            }
        });
        this.workerThread.start();
        this.reachable = true;
        
        this.timeoutThread = new Thread(new Runnable() {

            @Override
            public void run() {
                while(true) {
                    boolean cont = false;
                    if(requestQueue.size() > 0) {
                        ClassificationRequest ereq = requestQueue.peek();
                        Date cur = new Date();
                        if(ereq != null && cur.getTime() - ereq.getSentTime() >= QUERY_TIMEOUT) {
                            LOG.info("found timeout request");
                            //timeout
                            boolean timeout = false;
                            synchronized (ereq) {
                                ClassificationRequest.RequestStatus status = ereq.getStatus();
                                if(status.equals(ClassificationRequest.RequestStatus.STATUS_UNKNOWN)) {
                                    ereq.setStatus(ClassificationRequest.RequestStatus.STATUS_TIMEOUT);
                                    timeout = true;
                                }
                                
                                requestQueue.remove(ereq);
                            }
                            
                            synchronized (requestMap) {
                                requestMap.remove(ereq.getReqId());
                            }
                            
                            if(timeout) {
                                LOG.info("timeout : " + ereq.getReqId());
                                handler.onTimeout(ereq.getReqId(), ereq.getHeader(), ereq.getSequence());
                                
                                synchronized (requestQueue) {
                                    requestQueue.notifyAll();
                                }
                            }
                            cont = true;
                        }
                    }
                
                    if(!cont) {
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException ex) {
                            break;
                        }
                    }
                }
            }
        });
        this.timeoutThread.start();
    }
    
    public void waitForTasks() throws InterruptedException {
        synchronized (this.requestQueue) {
            while(this.requestQueue.size() != 0) {
                this.requestQueue.wait();
            }
        }
    }
    
    public boolean requestWouldBlock() {
        return this.requestQueue.size() == QUEUE_SIZE;
    }
    
    public void request(long reqId, String header, String sequence) throws IOException, InterruptedException {
        ClassificationRequest creq = new ClassificationRequest();
        creq.setReqId(reqId);
        creq.setHeader(header);
        creq.setSequence(sequence);
        
        // send
        this.requestQueue.put(creq);
        Date cur = new Date();
        creq.setSentTime(cur.getTime());
        
        synchronized (this.requestMap) {
            this.requestMap.put(reqId, creq);
        }
        
        LOG.info("req : " + reqId);
        
        ClassificationRequestMessage reqMsg = creq.getRequestMessage();
        
        AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder();
        AMQP.BasicProperties prop = builder.replyTo(this.queueName).build();
        
        synchronized (requestChannel) {
            this.requestChannel.basicPublish("", "request", prop, reqMsg.toJson().getBytes());
        }
    }
    
    public synchronized void reportTimeout() {
        this.timeout++;
        
        if(this.timeout >= TIMEOUT_MAX_COUNT) {
            this.reachable = false;
        }
    }
    
    public synchronized void setReachable(boolean reachable) {
        this.reachable = reachable;
    }
    
    public synchronized boolean isReachable() {
        return this.reachable;
    }

    @Override
    public void close() throws IOException {
        if(this.workerThread != null) {
            this.workerThread.interrupt();
            this.workerThread = null;
        }
        
        if(this.timeoutThread != null) {
            this.timeoutThread.interrupt();
            this.timeoutThread = null;
        }
        
        if(this.requestChannel != null) {
            try {
                this.requestChannel.close();
            } catch (TimeoutException ex) {
            }
            this.requestChannel = null;
        }
        
        if(this.responseChannel != null) {
            try {
                this.responseChannel.close();
            } catch (TimeoutException ex) {
            }
            this.responseChannel = null;
        }
        
        if(this.connection != null) {
            this.connection.close();
            this.connection = null;
        }
        
        this.requestQueue.clear();
        this.requestMap.clear();
        
        this.reachable = false;
    }
}
