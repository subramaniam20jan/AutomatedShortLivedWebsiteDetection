/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package patternMatcher;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import webcrawler.WebCrawler;

/**
 *
 * @author Subu
 */
public class IdentificationEngine {
    
    /*
     * Method to store all patterns that have matches in each webpages' source code.
     * 
     * Inserts the data corresponding to each webpage and pattern in the table
     * pattern_match.
     */
    public boolean storePatternMatches(Connection dbCon){
        ResultSet rs = null;
        ResultSet rs2 = null;
        Statement st = null;
        Statement st2 = null;
        List matchingPatterns = new ArrayList();
        
        try{
            st = dbCon.createStatement();
            st2 = dbCon.createStatement();
            rs = st.executeQuery("SELECT * FROM WPAGE WHERE WPAGE_AVAILABLE = 'Y'");
            
            while(rs.next()){
                matchingPatterns.clear();
                String wpageSource = rs.getString("WPAGE_CONTENT_HTML");
                rs2 = st2.executeQuery("SELECT * FROM pattern WHERE "
                        + "pattern_id NOT IN(SELECT pattern_id FROM wpage,pattern_match patmat WHERE "
                        + "wpage.wpage_id = patmat.wpage_id AND wpage.wpage_id = "+rs.getLong("WPAGE_ID") +")");
                while(rs2.next()){
                    String pattern = rs2.getString("PATTERN_VALUE").toLowerCase();
                    if(wpageSource.toLowerCase().contains(pattern))//matches(".*"+pattern+".*"))
                        matchingPatterns.add(rs2.getString("PATTERN_VALUE"));
                }
                rs2.close();
                
                //Insert matching patterns for each webpage
                insertMatches(dbCon,matchingPatterns,rs.getLong("WPAGE_ID"));
            }
        }
        catch(SQLException ex){
            Logger.getLogger(WebCrawler.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        finally{
            try{
                if(rs!=null && !rs.isClosed())
                    rs.close();
                if(rs2!=null&& !rs2.isClosed())
                    rs2.close();
                if(st!=null && !st.isClosed())
                    st.close();
                if(st2!=null && !st2.isClosed())
                    st2.close();
            }catch(Exception ignore){}
        }
        return true;
    }
    
    /*
     * Method to insert rows from the list matchingPatterns for webpage with
     * wpageID. 
     */
    private boolean insertMatches(Connection dbCon,List<String> matchingPatterns, long wpageID){
        ResultSet rs = null;
        Statement st = null;
        PreparedStatement pst = null;
        
        try{
            st = dbCon.createStatement();
            for(String pattern:matchingPatterns){
                rs = st.executeQuery("SELECT * FROM PATTERN,PATTERN_MATCH WHERE "
                        + "PATTERN.PATTERN_ID = PATTERN_MATCH.PATTERN_ID AND "
                        + "WPAGE_ID = "+wpageID+" AND PATTERN_VALUE = '"+pattern+"'");
                if(!rs.next()){
                    String insertStatement = "INSERT INTO PATTERN_MATCH(PATTERN_ID,WPAGE_ID) "
                            + "VALUES((SELECT PATTERN_ID FROM PATTERN WHERE PATTERN_VALUE =?),?)";
                    pst = dbCon.prepareStatement(insertStatement);
                    pst.setString(1, pattern);
                    pst.setLong(2, wpageID);
                    pst.execute();
                }
            }
            
        }
        catch(SQLException ex){
            Logger.getLogger(WebCrawler.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        finally{
            try{
                if(rs!=null)
                    rs.close();
                if(st!=null)
                    st.close();
                if(pst!=null)
                    pst.close();
            }catch(Exception ignore){}
        }
        
        return true;
    }
}
