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
package biospectra.classify;

import biospectra.classify.beans.ClassificationResult;
import biospectra.classify.beans.SearchResultEntry;
import biospectra.Configuration;
import biospectra.classify.beans.TaxonTreeDescription;
import biospectra.classify.beans.Taxonomy;
import biospectra.index.IndexConstants;
import biospectra.lucene.KmerQueryAnalyzer;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.analysis.CachingTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;

/**
 *
 * @author iychoi
 */
public class Classifier implements Closeable {
    
    private static final Log LOG = LogFactory.getLog(Classifier.class);
    
    private File indexPath;
    private KmerQueryAnalyzer queryAnalyzer;
    private IndexReader indexReader;
    private IndexSearcher indexSearcher;
    private double minShouldMatch;
    private int kmerSize;
    private int kmerSkips;
    private QueryGenerationAlgorithm queryGenerationAlgorithm;
    
    public Classifier(Configuration conf) throws Exception {
        if(conf == null) {
            throw new IllegalArgumentException("conf is null");
        }
        
        if(conf.getIndexPath() == null) {
            throw new IllegalArgumentException("indexPath is null");
        }
        
        if(conf.getKmerSize() <= 0) {
            throw new IllegalArgumentException("kmerSize must be larger than 0");
        }
        
        if(conf.getKmerSkips() <= 0) {
            throw new IllegalArgumentException("kmerSkips must be larger than 0");
        }
        
        initialize(new File(conf.getIndexPath()), conf.getKmerSize(), conf.getKmerSkips(), conf.getQueryMinShouldMatch(), conf.getQueryGenerationAlgorithm(), conf.getScoringAlgorithmObject());
    }
    
    private void initialize(File indexPath, int kmerSize, int kmerSkips, double minShouldMatch, QueryGenerationAlgorithm queryGenerationAlgorithm, Similarity similarity) throws Exception {
        if(!indexPath.exists() || !indexPath.isDirectory()) {
            throw new IllegalArgumentException("indexPath is not a directory or does not exist");
        }
        
        this.indexPath = indexPath;
        this.kmerSize = kmerSize;
        this.kmerSkips = kmerSkips;
        this.queryAnalyzer = new KmerQueryAnalyzer(this.kmerSize, this.kmerSkips);
        Directory dir = new MMapDirectory(this.indexPath.toPath()); 
        this.indexReader = DirectoryReader.open(dir);
        this.indexSearcher = new IndexSearcher(this.indexReader);
        if(similarity != null) {
            this.indexSearcher.setSimilarity(similarity);
        }
        this.minShouldMatch = minShouldMatch;
        this.queryGenerationAlgorithm = queryGenerationAlgorithm;
        
        BooleanQuery.setMaxClauseCount(10000);
    }

    private void createNaiveKmerQueryClauses(BooleanQuery.Builder builder, String field, CachingTokenFilter stream, TermToBytesRefAttribute termAtt, OffsetAttribute offsetAtt) throws IOException {
        while (stream.incrementToken()) {
            Term t = new Term(field, BytesRef.deepCopyOf(termAtt.getBytesRef()));
            builder.add(new TermQuery(t), BooleanClause.Occur.SHOULD);
        }
    }
    
    private void createChainProximityQueryClauses(BooleanQuery.Builder builder, String field, CachingTokenFilter stream, TermToBytesRefAttribute termAtt, OffsetAttribute offsetAtt) throws IOException {
        Term termArr[] = new Term[2];
        long offsetArr[] = new long[2];
        for(int i=0;i<2;i++) {
            termArr[i] = null;
            offsetArr[i] = 0;
        }

        while (stream.incrementToken()) {
            Term t = new Term(field, BytesRef.deepCopyOf(termAtt.getBytesRef()));
            if(termArr[0] == null) {
                termArr[0] = t;
                offsetArr[0] = offsetAtt.startOffset();
            } else if(termArr[1] == null) {
                termArr[1] = t;
                offsetArr[1] = offsetAtt.startOffset();
            } else {
                // shift
                termArr[0] = termArr[1];
                offsetArr[0] = offsetArr[1];
                // fill
                termArr[1] = t;
                offsetArr[1] = offsetAtt.startOffset();
            }
            
            long offsetDiff = offsetArr[1] - offsetArr[0];
            if(offsetDiff > 0) {
                PhraseQuery.Builder pq = new PhraseQuery.Builder();

                pq.setSlop((int) (offsetDiff) + 1);
                pq.add(termArr[0]);
                pq.add(termArr[1]);

                builder.add(pq.build(), BooleanClause.Occur.SHOULD);
            }

            termArr[0] = null;
            termArr[1] = null;
        }
    }

    private void createPairedProximityQueryClauses(BooleanQuery.Builder builder, String field, CachingTokenFilter stream, TermToBytesRefAttribute termAtt, OffsetAttribute offsetAtt) throws IOException {
        Term termArr[] = new Term[2];
        long offsetArr[] = new long[2];
        for(int i=0;i<2;i++) {
            termArr[i] = null;
            offsetArr[i] = 0;
        }

        int count = 0;
        while (stream.incrementToken()) {
            Term t = new Term(field, BytesRef.deepCopyOf(termAtt.getBytesRef()));
            if(count % 2 == 0) {
                termArr[0] = t;
                offsetArr[0] = offsetAtt.startOffset();
            } else {
                termArr[1] = t;
                offsetArr[1] = offsetAtt.startOffset();

                long offsetDiff = offsetArr[1] - offsetArr[0];
                if(offsetDiff > 0) {
                    PhraseQuery.Builder pq = new PhraseQuery.Builder();

                    pq.setSlop((int) (offsetDiff) + 1);
                    pq.add(termArr[0]);
                    pq.add(termArr[1]);

                    builder.add(pq.build(), BooleanClause.Occur.SHOULD);
                }

                termArr[0] = null;
                termArr[1] = null;
            }

            count++;
        }
        
        if(termArr[0] != null) {
            builder.add(new TermQuery(termArr[0]), BooleanClause.Occur.SHOULD);
            termArr[0] = null;
        }
    }
    
    protected BooleanQuery createQueryClauses(KmerQueryAnalyzer analyzer, String field, String queryText, QueryGenerationAlgorithm queryGenerationAlgorithm) {
        try (TokenStream source = analyzer.tokenStream(field, queryText);
            CachingTokenFilter stream = new CachingTokenFilter(source)) {

            TermToBytesRefAttribute termAtt = stream.getAttribute(TermToBytesRefAttribute.class);
            
            if (termAtt == null) {
                return null;
            }
            
            // phase 1: read through the stream and assess the situation:
            // counting the number of tokens/positions and marking if we have any synonyms.
            int numTokens = 0;

            stream.reset();
            while (stream.incrementToken()) {
                numTokens++;
            }

            // phase 2: based on token count, presence of synonyms, and options
            // formulate a single term, boolean, or phrase.
            if (numTokens == 0) {
                return null;
            } else if (numTokens == 1) {
                // single term
                return null;
            } else {
                BooleanQuery.Builder q = new BooleanQuery.Builder();
                q.setDisableCoord(false);
                
                TermToBytesRefAttribute termAttB = stream.getAttribute(TermToBytesRefAttribute.class);
                OffsetAttribute offsetAtt = stream.getAttribute(OffsetAttribute.class);

                stream.reset();
                
                if (queryGenerationAlgorithm.equals(QueryGenerationAlgorithm.NAIVE_KMER)) {
                    createNaiveKmerQueryClauses(q, field, stream, termAttB, offsetAtt);
                } else if(queryGenerationAlgorithm.equals(QueryGenerationAlgorithm.CHAIN_PROXIMITY)) {
                    createChainProximityQueryClauses(q, field, stream, termAttB, offsetAtt);
                } else if(queryGenerationAlgorithm.equals(QueryGenerationAlgorithm.PAIRED_PROXIMITY)) {
                    createPairedProximityQueryClauses(q, field, stream, termAttB, offsetAtt);
                }

                return q.build();
            }
        } catch (IOException e) {
            throw new RuntimeException("Error analyzing query text", e);
        }
    }
    
    protected BooleanQuery createQuery(KmerQueryAnalyzer analyzer, String field, String queryText, double minShouldMatch, QueryGenerationAlgorithm queryGenerationAlgorithm) {
        BooleanQuery queryClauses = createQueryClauses(analyzer, field, queryText, queryGenerationAlgorithm);
        BooleanQuery.Builder booleanQueryBuilder = new BooleanQuery.Builder();
        booleanQueryBuilder.setDisableCoord(queryClauses.isCoordDisabled());
        booleanQueryBuilder.setMinimumNumberShouldMatch((int) (minShouldMatch * queryClauses.clauses().size()));
        for (BooleanClause clause : queryClauses) {
            booleanQueryBuilder.add(clause);
        }
        return booleanQueryBuilder.build();
    }
    
    private ClassificationResult makeClassificationResult(String header, String sequence, List<SearchResultEntry> resultArr) throws IOException {
        if(resultArr == null || resultArr.isEmpty()) {
            return new ClassificationResult(header, sequence, null, ClassificationResult.ClassificationResultType.UNKNOWN, "unknown", "");
        } else if(resultArr.size() == 1) {
            SearchResultEntry entry = resultArr.get(0);
            String taxonHierarchy = entry.getTaxonHierarchy();
            if(taxonHierarchy != null && !taxonHierarchy.isEmpty()) {
                TaxonTreeDescription desc = TaxonTreeDescription.createInstance(taxonHierarchy);
                Taxonomy tax = desc.getLowestClassifiableTaxonomy();
                if(tax != null) {
                    return new ClassificationResult(header, sequence, resultArr, ClassificationResult.ClassificationResultType.CLASSIFIED, tax.getRank(), tax.getName());
                } else {
                    return new ClassificationResult(header, sequence, resultArr, ClassificationResult.ClassificationResultType.CLASSIFIED, "unknown", "");
                }
            } else {
                return new ClassificationResult(header, sequence, resultArr, ClassificationResult.ClassificationResultType.CLASSIFIED, "unknown", "");
            }
        } else {
            List<TaxonTreeDescription> descs = new ArrayList<TaxonTreeDescription>();
            for(SearchResultEntry entry : resultArr) {
                String taxonHierarchy = entry.getTaxonHierarchy();
                if(taxonHierarchy != null && !taxonHierarchy.isEmpty()) {
                    TaxonTreeDescription desc = TaxonTreeDescription.createInstance(taxonHierarchy);
                    descs.add(desc);
                } else {
                    // quick fail
                    return new ClassificationResult(header, sequence, resultArr, ClassificationResult.ClassificationResultType.VAGUE, "unknown", "");
                }
            }
            
            String rank = "";
            TaxonTreeDescription desc1 = descs.get(0);
            boolean classified = false;
            
            List<Taxonomy> tree1 = desc1.getClassifiableTaxonomyTree();
            
            Taxonomy classifiedTax = null;
            for(int idx=0;idx<tree1.size();idx++) {
                Taxonomy tax = tree1.get(idx);
            
                boolean foundCommonTaxRank = true;
                for(int j=1;j<descs.size();j++) {
                    TaxonTreeDescription desc_target = descs.get(j);
                    List<Taxonomy> desc_tree = desc_target.getClassifiableTaxonomyTree();
                    
                    boolean found = false;
                    for(Taxonomy tax_target : desc_tree) {
                        if(tax_target.getTaxid() == tax.getTaxid()) {
                            // found the same id
                            found = true;
                            break;
                        }
                    }
                    
                    if(!found) {
                        foundCommonTaxRank = false;
                        break;
                    }
                }
                
                if(foundCommonTaxRank) {
                    classified = true;
                    classifiedTax = tax;
                    break;
                }
            }
            
            if(classified) {
                if(classifiedTax != null) {
                    return new ClassificationResult(header, sequence, resultArr, ClassificationResult.ClassificationResultType.CLASSIFIED, classifiedTax.getRank(), classifiedTax.getName());
                } else {
                    return new ClassificationResult(header, sequence, resultArr, ClassificationResult.ClassificationResultType.CLASSIFIED, "unknown", "");
                }
            } else {
                return new ClassificationResult(header, sequence, resultArr, ClassificationResult.ClassificationResultType.VAGUE, "unknown", "");
            }
        }
    }
    
    public ClassificationResult classify(String header, String sequence) throws Exception {
        if(sequence == null || sequence.isEmpty()) {
            throw new IllegalArgumentException("sequence is null or empty");
        }
        
        ClassificationResult classificationResult = null;
        
        BooleanQuery q = createQuery(this.queryAnalyzer, IndexConstants.FIELD_SEQUENCE, sequence, this.minShouldMatch, this.queryGenerationAlgorithm);
        
        int hitsPerPage = 10;
        TopScoreDocCollector collector = TopScoreDocCollector.create(hitsPerPage);
        this.indexSearcher.search(q, collector);
        TopDocs topdocs = collector.topDocs();
        ScoreDoc[] hits = topdocs.scoreDocs;
        
        if(hits.length > 0) {
            List<SearchResultEntry> resultArr = new ArrayList<SearchResultEntry>();
            double topscore = topdocs.getMaxScore();
            for(int i=0;i<hits.length;++i) {
                if(topscore - hits[i].score == 0) {
                    int docId = hits[i].doc;
                    Document d = this.indexSearcher.doc(docId);
                    SearchResultEntry result = new SearchResultEntry(docId, d, i, hits[i].score);
                    resultArr.add(result);
                }
            }
            
            classificationResult = makeClassificationResult(header, sequence, resultArr);
        } else {
            classificationResult = makeClassificationResult(header, sequence, null);
        }
        
        return classificationResult;
    }
    
    @Override
    public void close() throws IOException {
        this.queryAnalyzer.close();
        this.indexReader.close();
    }
}
