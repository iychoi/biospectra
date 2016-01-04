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
public class RabbitMQInputServer implements Closeable {
    
    private static final Log LOG = LogFactory.getLog(RabbitMQInputServer.class);
    
    private ServerConfiguration conf;
    private RabbitMQInputServerEventHandler handler;
    private Connection connection;
    private Channel channel;
    private Consumer consumer;
    private Thread workerThread;

    public static abstract class RabbitMQInputServerEventHandler {
        protected abstract void handleMessage(RabbitMQInputRequest req, String replyTo);
    }
    
    public RabbitMQInputServer(ServerConfiguration conf, RabbitMQInputServerEventHandler handler) {
        if(conf == null) {
            throw new IllegalArgumentException("conf is null");
        }
        
        if(handler == null) {
            throw new IllegalArgumentException("handler is null");
        }
        
        this.conf = conf;
        this.handler = handler;
    }
    
    public void connect() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(this.conf.getHostname());
        factory.setPort(this.conf.getPort());
        factory.setUsername(this.conf.getUserId());
        factory.setPassword(this.conf.getUserPwd());
        
        factory.setAutomaticRecoveryEnabled(true);
        
        this.connection = factory.newConnection();
        this.channel = this.connection.createChannel();
        
        LOG.info("reader connected - " + this.conf.getHostname() + ":" + this.conf.getPort());
        
        this.channel.basicQos(1);
        this.channel.queueDeclare("request", false, false, true, null);
        
        this.consumer = new DefaultConsumer(this.channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, 
                    AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                
                LOG.debug("received - " + envelope.getRoutingKey() + ":" + message);
                
                if(handler != null) {
                    RabbitMQInputRequest req = RabbitMQInputRequest.createInstance(message);
                    handler.handleMessage(req, properties.getReplyTo());
                }
                
                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        };
        
        this.workerThread = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    channel.basicConsume("request", consumer);
                    LOG.info("Waiting for messages");
                } catch (IOException ex) {
                    LOG.error("Exception occurred while consuming a message", ex);
                }
            }
        });
        this.workerThread.start();
    }
    
    public void publishMessage(RabbitMQInputResponse res, String replyTo) {
        if(!this.channel.isOpen()) {
            throw new IllegalStateException("reader is not connected");
        }
        
        LOG.debug("return classification result - " + res.getReqId());
        try {
            this.channel.basicPublish("", replyTo, null, res.toJson().getBytes());
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
