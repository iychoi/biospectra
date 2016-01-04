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
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeoutException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class RabbitMQInputClient implements Closeable {
    
    private static final Log LOG = LogFactory.getLog(RabbitMQInputClient.class);
    
    private ClientConfiguration conf;
    private int hostId;
    private RabbitMQInputClientEventHandler handler;
    private Connection connection;
    private Channel channel;
    private Consumer consumer;
    private Thread workerThread;
    private String queueName;

    public static abstract class RabbitMQInputClientEventHandler {
        protected abstract void handleMessage(RabbitMQInputResponse res);
    }
    
    public RabbitMQInputClient(ClientConfiguration conf, int hostId, RabbitMQInputClientEventHandler handler) {
        if(conf == null) {
            throw new IllegalArgumentException("conf is null");
        }
        
        if(hostId < 0 || hostId >= conf.getHostnames().size()) {
            throw new IllegalArgumentException("hostId is invalid");
        }
        
        if(handler == null) {
            throw new IllegalArgumentException("handler is null");
        }
        
        this.conf = conf;
        this.hostId = hostId;
        this.handler = handler;
    }
    
    public void connect() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        String hostname = this.conf.getHostnames().get(this.hostId);
        factory.setHost(hostname);
        factory.setPort(this.conf.getPort());
        factory.setUsername(this.conf.getUserId());
        factory.setPassword(this.conf.getUserPwd());
        
        factory.setAutomaticRecoveryEnabled(true);
        
        this.connection = factory.newConnection();
        this.channel = this.connection.createChannel();
        
        LOG.info("reader connected - " + hostname + ":" + this.conf.getPort());
        
        this.channel.basicQos(1);
        this.queueName = this.channel.queueDeclare().getQueue();
        
        this.consumer = new DefaultConsumer(this.channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, 
                    AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                
                LOG.debug("received - " + envelope.getRoutingKey() + ":" + message);
                
                if(handler != null) {
                    RabbitMQInputResponse res = RabbitMQInputResponse.createInstance(message);
                    handler.handleMessage(res);
                }
                
                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        };
        
        this.workerThread = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    channel.basicConsume(queueName, consumer);
                    LOG.info("Waiting for messages");
                } catch (IOException ex) {
                    LOG.error("Exception occurred while consuming a message", ex);
                }
            }
        });
        this.workerThread.start();
    }
    
    public void request(RabbitMQInputRequest req) {
        if(!this.channel.isOpen()) {
            throw new IllegalStateException("reader is not connected");
        }
        
        LOG.debug("request classification - " + req.getReqId());
        try {
            AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder();
            AMQP.BasicProperties prop = builder.replyTo(this.queueName).build();
            this.channel.basicPublish("", "request", prop, req.toJson().getBytes());
        } catch (IOException ex) {
            LOG.error("Cannot publish", ex);
        }
    }

    @Override
    public void close() throws IOException {
        if(this.workerThread != null) {
            this.workerThread = null;
        }
        
        if(this.channel != null) {
            if(this.channel.isOpen()) {
                try {
                    this.channel.close();
                } catch (TimeoutException ex) {
                }
            }
            this.channel = null;
        }
        
        if(this.connection != null) {
            if(this.connection.isOpen()) {
                this.connection.close();
            }
            this.connection = null;
        }
    }
}
