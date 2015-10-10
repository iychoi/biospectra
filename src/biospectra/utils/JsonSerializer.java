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
package biospectra.utils;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

/**
 *
 * @author iychoi
 */
public class JsonSerializer {
    
    private ObjectMapper mapper;
            
    public JsonSerializer() {
        this.mapper = new ObjectMapper();
    }
    
    public JsonSerializer(boolean prettyformat) {
        this.mapper = new ObjectMapper();
        this.mapper.configure(SerializationConfig.Feature.INDENT_OUTPUT, prettyformat);
    }
    
    public String toJson(Object obj) throws IOException {
        StringWriter writer = new StringWriter();
        this.mapper.writeValue(writer, obj);
        return writer.getBuffer().toString();
    }
    
    public void toJsonFile(File f, Object obj) throws IOException {
        this.mapper.writeValue(f, obj);
    }
    
    public Object fromJson(String json, Class<?> cls) throws IOException {
        if(json == null) {
            return null;
        }
        StringReader reader = new StringReader(json);
        return this.mapper.readValue(reader, cls);
    }
    
    public Object fromJsonFile(File f, Class<?> cls) throws IOException {
        return this.mapper.readValue(f, cls);
    }
}
