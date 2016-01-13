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
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class ClientConfiguration {
    private static final Log LOG = LogFactory.getLog(ClientConfiguration.class);
    
    private List<String> rabbitmq_hostnames = new ArrayList<String>();
    private int rabbitmq_port;
    private String rabbitmq_userId;
    private String rabbitmq_userPwd;
    
    public static ClientConfiguration createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }

        JsonSerializer serializer = new JsonSerializer();
        return (ClientConfiguration) serializer.fromJsonFile(file, ClientConfiguration.class);
    }
    
    public static ClientConfiguration createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is empty or null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (ClientConfiguration) serializer.fromJson(json, ClientConfiguration.class);
    }
    
    public ClientConfiguration() {
        
    }
    
    @JsonIgnore
    public void addRabbitMQHostname(String hostname) {
        this.rabbitmq_hostnames.add(hostname);
    }
    
    @JsonProperty("rabbitmq_hostnames")
    public void addRabbitMQHostname(List<String> hostnames) {
        this.rabbitmq_hostnames.addAll(hostnames);
    }

    @JsonProperty("rabbitmq_hostnames")
    public List<String> getRabbitMQHostnames() {
        return this.rabbitmq_hostnames;
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
