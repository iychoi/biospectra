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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.kohsuke.args4j.Option;

/**
 *
 * @author iychoi
 */
public class CommandArgumentIndex extends CommandArgumentsBase {
    private static final Log LOG = LogFactory.getLog(CommandArgumentIndex.class);
    
    @Option(name = "-j", aliases = "--json", usage = "pass json configuration file")
    protected String jsonConfiguration;
    
    @Option(name = "-r", aliases = "--ref", usage = "specify reference FASTA path")
    protected String referenceDir;
    
    @Option(name = "-i", aliases = "--index", usage = "specify index path to be created")
    protected String indexDir;
    
    public String getReferenceDir() {
        return this.referenceDir;
    }
    
    public String getIndexDir() {
        return this.indexDir;
    }
    
    public String getJsonConfiguration() {
        return this.jsonConfiguration;
    }
    
    @Override
    public boolean checkValidity() {
        if(this.referenceDir == null || this.referenceDir.isEmpty()) {
            return false;
        }
        
        if(this.jsonConfiguration == null || this.jsonConfiguration.isEmpty()) {
            if(this.indexDir == null || this.indexDir.isEmpty()) {
                return false;
            }
        }
        
        return true;
    }
    
    @Override
    public String getValidityErrorMessage() {
        if(this.referenceDir == null || this.referenceDir.isEmpty()) {
            return "Reference dataset path is not given";
        }
        
        if(this.jsonConfiguration == null || this.jsonConfiguration.isEmpty()) {
            if(this.indexDir == null || this.indexDir.isEmpty()) {
                return "Index path to be created is not given";
            }
        }
        return null;
    }
}
