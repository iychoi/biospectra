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
import com.rabbitmq.client.ReturnListener;
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
    private Channel requestChannel;
    private Channel responseChannel;
    private Consumer consumer;
    private Thread workerThread;

    public static abstract class RabbitMQInputServerEventHandler {
        protected abstract void handleMessage(ClassificationRequestMessage req, String replyTo);
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
    
    public synchronized void connect() throws IOException, TimeoutException {
        LOG.info("Connecting to - " + this.conf.getRabbitMQHostname() + ":" + this.conf.getRabbitMQPort());
        
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(this.conf.getRabbitMQHostname());
        factory.setPort(this.conf.getRabbitMQPort());
        factory.setUsername(this.conf.getRabbitMQUserId());
        factory.setPassword(this.conf.getRabbitMQUserPwd());
        
        factory.setAutomaticRecoveryEnabled(true);
        
        this.connection = factory.newConnection();
        this.requestChannel = this.connection.createChannel();
        this.responseChannel = this.connection.createChannel();
        
        LOG.info("connected.");
        
        this.requestChannel.basicQos(1);
        this.requestChannel.queueDeclare("request", false, false, true, null);
        
        this.responseChannel.addReturnListener(new ReturnListener() {

            @Override
            public void handleReturn(int replyCode, String replyText, String exchange, String routingKey, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                LOG.info("message not delivered to " + routingKey);
                LOG.info(message);
            }
        });
        
        this.consumer = new DefaultConsumer(this.requestChannel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, 
                    AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                
                this.getChannel().basicAck(envelope.getDeliveryTag(), false);
                
                if(handler != null) {
                    ClassificationRequestMessage req = ClassificationRequestMessage.createInstance(message);
                    handler.handleMessage(req, properties.getReplyTo());
                }
            }
        };
        
        this.workerThread = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    requestChannel.basicConsume("request", consumer);
                    LOG.info("Waiting for messages");
                } catch (IOException ex) {
                    LOG.error("Exception occurred while consuming a message", ex);
                }
            }
        });
        this.workerThread.start();
    }
    
    public synchronized void publishMessage(ClassificationResponseMessage res, String replyTo) {
        try {
            this.responseChannel.basicPublish("", replyTo, true, false, null, res.toJson().getBytes());
        } catch (IOException ex) {
            LOG.error("Cannot publish", ex);
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if(this.workerThread != null) {
            this.workerThread.interrupt();
            this.workerThread = null;
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
    }
}
