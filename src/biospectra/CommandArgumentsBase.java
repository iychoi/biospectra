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
public class CommandArgumentsBase {
    
    private static final Log LOG = LogFactory.getLog(CommandArgumentsBase.class);
    
    @Option(name = "-h", aliases = "--help", usage = "print help message") 
    protected boolean help = false;

    public boolean isHelp() {
        return this.help;
    }
    
    @Override
    public String toString() {
        return super.toString();
    }
    
    public boolean checkValidity() {
        return true;
    }
    
    public String getValidityErrorMessage() {
        return null;
    }
}
