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
public class CommandArgumentClassifierClient extends CommandArgumentsBase {
    private static final Log LOG = LogFactory.getLog(CommandArgumentClassifierClient.class);
    
    @Option(name = "-j", aliases = "--json", usage = "pass json configuration file")
    protected String jsonConfiguration;
    
    @Option(name = "-in", aliases = "--input", usage = "query FASTA path")
    protected String inputDir;
    
    @Option(name = "-out", aliases = "--output", usage = "output path")
    protected String outputDir;
    
    public String getInputDir() {
        return this.inputDir;
    }
    
    public String getOutputDir() {
        return this.outputDir;
    }
    
    public String getJsonConfiguration() {
        return this.jsonConfiguration;
    }
    
    @Override
    public boolean checkValidity() {
        if(this.inputDir == null || this.inputDir.isEmpty()) {
            return false;
        }
        
        if(this.outputDir == null || this.outputDir.isEmpty()) {
            return false;
        }
        
        if(this.jsonConfiguration == null || this.jsonConfiguration.isEmpty()) {
            return false;
        }
        
        return true;
    }
    
    @Override
    public String getValidityErrorMessage() {
        if(this.inputDir == null || this.inputDir.isEmpty()) {
            return "Query input path is not given";
        }
        
        if(this.outputDir == null || this.outputDir.isEmpty()) {
            return "Output path is not given";
        }
        
        if(this.jsonConfiguration == null || this.jsonConfiguration.isEmpty()) {
            return "Configuration file is not given";
        }
        
        return null;
    }
}
