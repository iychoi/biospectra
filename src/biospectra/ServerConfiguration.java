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
package biospectra;

import biospectra.utils.JsonSerializer;
import java.io.File;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class ServerConfiguration extends Configuration {
    private static final Log LOG = LogFactory.getLog(ServerConfiguration.class);
    
    private String rabbitmq_hostname;
    private int rabbitmq_port;
    private String rabbitmq_userId;
    private String rabbitmq_userPwd;
    
    public static ServerConfiguration createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }

        JsonSerializer serializer = new JsonSerializer();
        return (ServerConfiguration) serializer.fromJsonFile(file, ServerConfiguration.class);
    }
    
    public static ServerConfiguration createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is empty or null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (ServerConfiguration) serializer.fromJson(json, ServerConfiguration.class);
    }
    
    public ServerConfiguration() {
        
    }
    
    @JsonProperty("rabbitmq_hostname")
    public void setRabbitMQHostname(String hostname) {
        this.rabbitmq_hostname = hostname;
    }

    @JsonProperty("rabbitmq_hostname")
    public String getRabbitMQHostname() {
        return this.rabbitmq_hostname;
    }

    @JsonProperty("rabbitmq_port")
    public void setRabbitMQPort(int port) {
        this.rabbitmq_port = port;
    }
    
    @JsonProperty("rabbitmq_port")
    public int getRabbitMQPort() {
        return this.rabbitmq_port;
    }

    @JsonProperty("rabbitmq_user_id")
    public void setRabbitMQUserId(String userId) {
        this.rabbitmq_userId = userId;
    }

    @JsonProperty("rabbitmq_user_id")
    public String getRabbitMQUserId() {
        return this.rabbitmq_userId;
    }

    @JsonProperty("rabbitmq_user_pwd")
    public void setRabbitMQUserPwd(String userPwd) {
        this.rabbitmq_userPwd = userPwd;
    }
    
    @JsonProperty("rabbitmq_user_pwd")
    public String getRabbitMQUserPwd() {
        return this.rabbitmq_userPwd;
    }

    @JsonIgnore
    public synchronized String toJson() throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return serializer.toJson(this);
    }
    
    @JsonIgnore
    public synchronized void saveTo(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        serializer.toJsonFile(file, this);
    }
}
