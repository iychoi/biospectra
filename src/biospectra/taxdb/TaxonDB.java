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
package biospectra.taxdb;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class TaxonDB implements Closeable {
    
    private static final Log LOG = LogFactory.getLog(TaxonDB.class);
    
    private static final String QUERY_BY_TAXID = "select taxid, name, parent, rank from tree where taxid = ?";
    private static final String QUERY_BY_GI = "select t.taxid as taxid, t.name as name, t.parent as parent, t.rank as rank from gi_taxid g, tree t where g.taxid = t.taxid and g.gi = ?";
    
    private File dbPath;
    private Connection connection;
    
    static {
        try {
            // load sqlite-jdbc driver
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException ex) {
            LOG.error(ex);
        }
    }
    
    public TaxonDB(String dbPath) throws Exception {
        if(dbPath == null) {
            throw new IllegalArgumentException("dbPath is null");
        }
        
        initialize(new File(dbPath));
    }
    
    public TaxonDB(File dbPath) throws Exception {
        initialize(dbPath);
    }
    
    private void initialize(File dbPath) throws Exception {
        if(dbPath == null) {
            throw new IllegalArgumentException("dbPath is null");
        }
        
        this.dbPath = dbPath;
        
        try {
            this.connection = DriverManager.getConnection("jdbc:sqlite:" + dbPath.getAbsolutePath());
        } catch (SQLException ex) {
            throw ex;
        }
    }
    
    public Taxonomy getTaxonomyByTaxid(int taxid) throws Exception {
        Taxonomy tax = null;
        PreparedStatement pstmt = null;
        try {
            pstmt = this.connection.prepareStatement(QUERY_BY_TAXID);
            pstmt.setInt(1, taxid);
            ResultSet rs = pstmt.executeQuery();
            while(rs.next()) {
                tax = new Taxonomy(rs.getInt("taxid"), rs.getString("name"), rs.getInt("parent"), rs.getString("rank"));
                break;
            }
        } catch (SQLException ex) {
            throw ex;
        } finally {
            if(pstmt != null) {
                pstmt.close();
            }
        }
        
        return tax;
    }
    
    public Taxonomy getTaxonomyByGI(int gi) throws Exception {
        Taxonomy tax = null;
        PreparedStatement pstmt = null;
        try {
            pstmt = this.connection.prepareStatement(QUERY_BY_GI);
            pstmt.setInt(1, gi);
            ResultSet rs = pstmt.executeQuery();
            while(rs.next()) {
                tax = new Taxonomy(rs.getInt("taxid"), rs.getString("name"), rs.getInt("parent"), rs.getString("rank"));
                break;
            }
        } catch (SQLException ex) {
            throw ex;
        } finally {
            if(pstmt != null) {
                pstmt.close();
            }
        }
        
        return tax;
    }
    
    public List<Taxonomy> getFullTaxonomyHierarchyByTaxid(int taxid) throws Exception {
        List<Taxonomy> taxHier = new ArrayList<Taxonomy>();
        int next_taxid = taxid;
        boolean hasParent = true;
        do {
            Taxonomy tax = getTaxonomyByTaxid(next_taxid);
            if(tax == null) {
                hasParent = false;
                break;
            }
            
            taxHier.add(0, tax);
            
            if(tax.getParent() == tax.getTaxid()) {
                // self?
                hasParent = false;
                break;
            }
            
            next_taxid = tax.getParent();
        } while(hasParent);
        
        return Collections.unmodifiableList(taxHier);
    }
    
    public List<Taxonomy> getFullTaxonomyHierarchyByGI(int gi) throws Exception {
        List<Taxonomy> taxHier = new ArrayList<Taxonomy>();
        
        Taxonomy taxByGI = getTaxonomyByGI(gi);
        if(taxByGI != null) {
            taxHier.add(taxByGI);
            
            int next_taxid = taxByGI.getParent();
            boolean hasParent = true;
            do {
                Taxonomy tax = getTaxonomyByTaxid(next_taxid);
                if (tax == null) {
                    hasParent = false;
                    break;
                }

                taxHier.add(0, tax);

                if (tax.getParent() == tax.getTaxid()) {
                    // self?
                    hasParent = false;
                    break;
                }

                next_taxid = tax.getParent();
            } while (hasParent);
        }
        
        return Collections.unmodifiableList(taxHier);
    }

    @Override
    public void close() throws IOException {
        try {
            this.connection.close();
        } catch (SQLException ex) {
            throw new IOException(ex);
        }
    }
}
