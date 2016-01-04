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

import biospectra.classify.server.RabbitMQInputReceiverConstants;
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
    
    private String hostname;
    private int port;
    private String userId;
    private String userPwd;
    private String exchange = RabbitMQInputReceiverConstants.EXCHANGE_NAME;
    private String queue = RabbitMQInputReceiverConstants.QUEUE_NAME;
    
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
    
    @JsonProperty("hostname")
    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    @JsonProperty("hostname")
    public String getHostname() {
        return this.hostname;
    }

    @JsonProperty("port")
    public void setPort(int port) {
        this.port = port;
    }
    
    @JsonProperty("port")
    public int getPort() {
        return this.port;
    }

    @JsonProperty("user_id")
    public void setUserId(String userId) {
        this.userId = userId;
    }

    @JsonProperty("user_id")
    public String getUserId() {
        return this.userId;
    }

    @JsonProperty("user_pwd")
    public void setUserPwd(String userPwd) {
        this.userPwd = userPwd;
    }
    
    @JsonProperty("user_pwd")
    public String getUserPwd() {
        return this.userPwd;
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
