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

import biospectra.classify.ClassifierClient;
import biospectra.classify.LocalClassifier;
import biospectra.classify.server.ClassifierServer;
import biospectra.utils.IndexUtil;
import biospectra.index.Indexer;
import biospectra.taxdb.TaxonDB;
import biospectra.taxdb.Taxonomy;
import biospectra.utils.FastaFileHelper;
import biospectra.verification.MetagenomicReadGenerator;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Date;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.yeastrc.fasta.FASTAEntry;

/**
 *
 * @author iychoi
 */
public class BioSpectra {

    private static final Log LOG = LogFactory.getLog(BioSpectra.class);
    
    private static void printHelp() {
        System.err.println("Help:");
        System.err.println("> mode(first parameter) must be one of followings:");
        System.err.println("> \'i\' (OR \'index\') - construct index from references");
        System.err.println("> \'lc\' (OR \'lclassify\') - classify metagenomic samples");
        System.err.println("> \'rc\' (OR \'rclassify\') - classify metagenomic samples through server");
        System.err.println("> \'svr\' (OR \'server\') - run classification server");
    }
    
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {
        if(args.length < 1) {
            printHelp();
            return;
        }
        
        String programMode = args[0];
        String[] programArgs = new String[args.length - 1];
        System.arraycopy(args, 1, programArgs, 0, args.length - 1);
        
        if(programMode.equalsIgnoreCase("i") || programMode.equalsIgnoreCase("index")) {
            System.out.println("Indexing...");
            CommandArgumentsParser<CommandArgumentIndex> parser = new CommandArgumentsParser<CommandArgumentIndex>();
            CommandArgumentIndex indexArg = new CommandArgumentIndex();
            if(!parser.parse(programArgs, indexArg)) {
                return;
            }
            
            index(indexArg);
        } else if(programMode.equalsIgnoreCase("lc") || programMode.equalsIgnoreCase("lclassify")) {
            CommandArgumentsParser<CommandArgumentLocalClassifier> parser = new CommandArgumentsParser<CommandArgumentLocalClassifier>();
            CommandArgumentLocalClassifier classifierArg = new CommandArgumentLocalClassifier();
            if(!parser.parse(programArgs, classifierArg)) {
                return;
            }
            
            classifyLocal(classifierArg);
        } else if(programMode.equalsIgnoreCase("rc") || programMode.equalsIgnoreCase("rclassify")) {
            CommandArgumentsParser<CommandArgumentClassifierClient> parser = new CommandArgumentsParser<CommandArgumentClassifierClient>();
            CommandArgumentClassifierClient clientArg = new CommandArgumentClassifierClient();
            if(!parser.parse(programArgs, clientArg)) {
                return;
            }
            
            classifyRemote(clientArg);
        } else if(programMode.equalsIgnoreCase("svr") || programMode.equalsIgnoreCase("server")) {
            CommandArgumentsParser<CommandArgumentServer> parser = new CommandArgumentsParser<CommandArgumentServer>();
            CommandArgumentServer serverArg = new CommandArgumentServer();
            if(!parser.parse(programArgs, serverArg)) {
                return;
            }
            
            runServer(serverArg);
        } else {
            printHelp();
        }
    }

    private static void index(CommandArgumentIndex arg) throws Exception {
        Configuration conf = null;
        
        if(arg.getJsonConfiguration() != null && !arg.getJsonConfiguration().isEmpty()) {
            conf = Configuration.createInstance(new File(arg.getJsonConfiguration()));
        } else {
            conf = new Configuration();
            conf.setIndexPath(arg.getIndexDir());
        }
        
        Indexer indexer = new Indexer(conf);
        
        List<File> refereneFiles = FastaFileHelper.findFastaDocs(arg.getReferenceDir());
        for(File fastaDoc : refereneFiles) {
            LOG.info("indexing " + fastaDoc.getAbsolutePath() + " started");
            Date start = new Date();
            
            indexer.index(fastaDoc);
            
            Date end = new Date(); 
            LOG.info("indexing " + fastaDoc.getAbsolutePath() + " finished - " + (end.getTime() - start.getTime()) + " total milliseconds");
        }
        
        indexer.close();
    }
    
    private static void classifyLocal(CommandArgumentLocalClassifier arg) throws Exception {
        Configuration conf = null;
        
        if(arg.getJsonConfiguration() != null && !arg.getJsonConfiguration().isEmpty()) {
            conf = Configuration.createInstance(new File(arg.getJsonConfiguration()));
        } else {
            conf = new Configuration();
            conf.setIndexPath(arg.getIndexDir());
        }
        
        LocalClassifier classifier = new LocalClassifier(conf);
        
        File output = new File(arg.getOutputDir());
        if(!output.exists()) {
            output.mkdirs();
        }
        
        List<File> fastaDocs = FastaFileHelper.findFastaDocs(arg.getInputDir());
        for(File fastaDoc : fastaDocs) {
            File resultOutput = new File(arg.getOutputDir() + "/" + fastaDoc.getName() + ".result");
            File sumResultOutput = new File(arg.getOutputDir() + "/" + fastaDoc.getName() + ".result.sum");
            classifier.classify(fastaDoc, resultOutput, sumResultOutput);
        }
        
        classifier.close();
    }
    
    private static void classifyRemote(CommandArgumentClassifierClient arg) throws Exception {
        ClientConfiguration conf = ClientConfiguration.createInstance(new File(arg.getJsonConfiguration()));
        
        ClassifierClient classifier = new ClassifierClient(conf);
        classifier.start();
        
        File output = new File(arg.getOutputDir());
        if(!output.exists()) {
            output.mkdirs();
        }
        
        List<File> fastaDocs = FastaFileHelper.findFastaDocs(arg.getInputDir());
        for(File fastaDoc : fastaDocs) {
            File resultOutput = new File(arg.getOutputDir() + "/" + fastaDoc.getName() + ".result");
            File sumResultOutput = new File(arg.getOutputDir() + "/" + fastaDoc.getName() + ".result.sum");
            classifier.classify(fastaDoc, resultOutput, sumResultOutput);
        }
        
        classifier.close();
    }
    
    private static void runServer(CommandArgumentServer arg) throws Exception {
        ServerConfiguration conf = ServerConfiguration.createInstance(new File(arg.getJsonConfiguration()));
        
        ClassifierServer server = new ClassifierServer(conf);
        server.start();
        
        while(!Thread.currentThread().isInterrupted()) {
            try {
                // service loop
                Thread.sleep(1000);
            } catch(InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }
        
        server.close();
    }
    
    private static void sample(String[] args) throws Exception {
        String fastaDir = args[1];
        int readSize = Integer.parseInt(args[2]);
        double errorRatioStart = Double.parseDouble(args[3]);
        double errorRatioEnd = Double.parseDouble(args[4]);
        double errorRatioStep = Double.parseDouble(args[5]);
        int iteration = Integer.parseInt(args[6]);
        String outDir = args[7];

        List<File> fastaDocs = FastaFileHelper.findFastaDocs(fastaDir);

        File outDirFile = new File(outDir);
        if(!outDirFile.exists()) {
            outDirFile.mkdirs();
        }
        
        MetagenomicReadGenerator generator = new MetagenomicReadGenerator(fastaDocs);
        for (double cur = errorRatioStart; cur <= errorRatioEnd; cur += errorRatioStep) {
            File outFile = new File(outDir + "/sample_" + cur + ".fa");
            BufferedWriter bw = new BufferedWriter(new FileWriter(outFile));
            for(int i=0;i<iteration;i++) {
                FASTAEntry sample = generator.generate(readSize, cur);
                bw.write(sample.getHeaderLine());
                bw.newLine();
                bw.write(sample.getSequence());
                bw.newLine();
            }
            bw.close();
        }
    }

    private static void utils(String[] args) throws Exception {
        String operation = args[1];
        
        if(operation.equalsIgnoreCase("index")) {
            String indexDir = args[2];
        
            IndexUtil indexutil = new IndexUtil(indexDir);
            System.out.println("total docs : " + indexutil.countDocs());
            indexutil.close();
        } else if(operation.equalsIgnoreCase("fasta")) {
            String fastaDir = args[2];
        
            int count = 0;
            long size_sum = 0;
            List<File> fastaDocs = FastaFileHelper.findFastaDocs(fastaDir);
            for(File fastaDoc : fastaDocs) {
                System.out.println("found FASTA file : " + fastaDoc.getAbsolutePath());
                System.out.println("size> " + fastaDoc.length());
                count++;
                size_sum += fastaDoc.length();
            }
            System.out.println("SUMMARY");
            System.out.println("Count : " + count);
            System.out.println("Size in total : " + size_sum);
        } else if(operation.equalsIgnoreCase("nfasta")) {
            String fastaDir = args[2];
        
            int count = 0;
            long size_sum = 0;
            List<File> fastaDocs = FastaFileHelper.findNonFastaDocs(fastaDir);
            for(File fastaDoc : fastaDocs) {
                System.out.println("found non-FASTA file : " + fastaDoc.getAbsolutePath());
                System.out.println("size> " + fastaDoc.length());
                count++;
                size_sum += fastaDoc.length();
            }
            System.out.println("SUMMARY");
            System.out.println("Count : " + count);
            System.out.println("Size in total : " + size_sum);
        } else if(operation.equalsIgnoreCase("taxid")) {
            String dbDir = args[2];
            int taxid = Integer.parseInt(args[3]);
            
            TaxonDB db = new TaxonDB(dbDir);
            List<Taxonomy> taxon = db.getFullTaxonomyHierarchyByTaxid(taxid);
            for(Taxonomy t : taxon) {
                System.out.println(t.toString());
            }
            db.close();
        } else if(operation.equalsIgnoreCase("gi")) {
            String dbFile = args[2];
            int gi = Integer.parseInt(args[3]);
            
            TaxonDB db = new TaxonDB(dbFile);
            List<Taxonomy> taxon = db.getFullTaxonomyHierarchyByGI(gi);
            for(Taxonomy t : taxon) {
                System.out.println(t.toString());
            }
            db.close();
        }
    }
}
