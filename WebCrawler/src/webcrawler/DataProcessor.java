/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package webcrawler;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Subu
 * 
 * This class contains the code needed to process the Jaccard distance between website pairs based on various
 * parameters for each web page.
 * 
 * The Jaccard distance between two sets S and T is defined as
 * 1- J(S, T), where:
 * 
 * J(S, T) = |S ∩ T|
             |S ∪ T|
 * 
 * Consider comparing website similarity by sentences. If website A has 50 sentences in the text of its web pages and
 * website B has 40 sentences, and they have 35 in common, then the Jaccard distance is 1 − J(A, B)=1 − 35/65 = 0.46.
 */
public class DataProcessor {
    
    /*
     * Contains the list of strings that will be ignored while builting the vocab dictionary for Jdist between
     * two pages in the method storeVocabJDist
     */
    private static List<String> commonExclStrings = new ArrayList();
    
    public DataProcessor(Connection dbCon) throws SQLException{
        ResultSet rs = null;
        Statement st = null;
        try{
            st = dbCon.createStatement();
            rs = st.executeQuery("SELECT * FROM BLACKLIST WHERE BLACKLIST_TYPE='JDIST_VOCAB'");
            while(rs.next()){
                commonExclStrings.add(rs.getString("BLACKLIST_VALUE"));
            }
        }catch(SQLException ex){
            Logger.getLogger(WebCrawler.class.getName()).log(Level.SEVERE, null, ex);
            throw ex;
        }
        finally{
            try{
                if(rs!=null)
                    rs.close();
                if(st!=null)
                    st.close();
            }catch(Exception ignore){}
        }
        
    }
    
    /*
     * Method to compute Jaccard distance between two sets provided as an argument
     */
    public static float getJaccardDistance(Set set1,Set set2){
        float ret;
        float common=0;
        float union;
        
        if(set1.isEmpty() || set2.isEmpty())
            return 1;
        
        for(Object item:set2)
        {
            if(set1.contains(item))
                common++;
        }
        
        int size1 = set1.size();
        int size2 = set2.size();
        
        union = size1+size2-common;
        
        ret = 1-(common/union);
        
        return ret;
    }
    
    /*
     * Method used to store the Jdist value between the builtwith attributes of two pages with id pageID1 and pageID2
     */
    public boolean storeBuiltwithJDist(Connection dbCon,long pageID1, long pageID2){
        Set set1 = new HashSet();
        Set set2 = new HashSet();
        ResultSet rs = null;
        Statement st = null;
        PreparedStatement pst = null;
        
        //If both pages are the same, return false and dont store any values
        if(pageID1 == pageID2)
            return false;
        
        //Always keep pageID1 as the smaller value to remove duplicates
        if(pageID1 > pageID2){
            long tmp = pageID1;
            pageID1 = pageID2;
            pageID2 = tmp;
        }
        
        try{
            st = dbCon.createStatement();
            rs = st.executeQuery("SELECT DISTINCT technology_name,technology_value FROM  builtwith_detail det, builtwith_key bkey "
                    + "WHERE det.builtwith_id = bkey.builtwith_id AND wpage_id = "+pageID1);
            
            while(rs.next()){
                set1.add(rs.getString("TECHNOLOGY_NAME"));
            }
            rs.close();
            
            rs = st.executeQuery("SELECT DISTINCT technology_name,technology_value FROM  builtwith_detail det, builtwith_key bkey "
                    + "WHERE det.builtwith_id = bkey.builtwith_id AND wpage_id = "+pageID2);
            
            while(rs.next()){
                set2.add(rs.getString("TECHNOLOGY_NAME"));
            }
            rs.close();
            
            float jDist = getJaccardDistance(set1,set2);
            
            rs = st.executeQuery("SELECT * FROM WPAGE_JDIST WHERE WPAGE1_ID = "+pageID1+" AND WPAGE2_ID = "+pageID2
                    +" AND JDIST_TYPE ='BUILTWITH'");
            
            if(rs.next()){
                String insertStatement = "UPDATE WPAGE_JDIST SET JDIST_VALUE = ? WHERE WPAGE1_ID=? AND "
                        + "WPAGE2_ID=? AND JDIST_TYPE=? ";
                pst = dbCon.prepareStatement(insertStatement);
                pst.setLong(2, pageID1);
                pst.setLong(3, pageID2);
                pst.setFloat(1, jDist);
                pst.setString(4, "BUILTWITH");
                pst.executeUpdate();
            }
            else{
                String insertStatement = "INSERT INTO WPAGE_JDIST(WPAGE1_ID,WPAGE2_ID,JDIST_VALUE,JDIST_TYPE) "
                    + "VALUES(?,?,?,?)";
                pst = dbCon.prepareStatement(insertStatement);
                pst.setLong(1, pageID1);
                pst.setLong(2, pageID2);
                pst.setFloat(3, jDist);
                pst.setString(4, "BUILTWITH");
                pst.executeUpdate();
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
    
    /*
     * Method used to store the Jdist value between the text attributes (paragraphs, text areas and lists)
     * of two pages with id pageID1 and pageID2
     */
    public boolean storeHTextJDist(Connection dbCon,long pageID1, long pageID2){
        Set set1 = new HashSet();
        Set set2 = new HashSet();
        ResultSet rs = null;
        Statement st = null;
        PreparedStatement pst = null;
        
        //If both pages are the same, return false and dont store any values
        if(pageID1 == pageID2)
            return false;
        
        //Always keep pageID1 as the smaller value to remove duplicates
        if(pageID1 > pageID2){
            long tmp = pageID1;
            pageID1 = pageID2;
            pageID2 = tmp;
        }
        
        try{
            st = dbCon.createStatement();
            rs = st.executeQuery("SELECT * FROM (SELECT meta_data_field,length(meta_data_value)len,meta_data_value,wpage_id FROM wpage_metadata) "
                    + "WHERE meta_data_field LIKE 'HTML_TEXT%' AND ((meta_data_field IN ('HTML_TEXT_li','HTML_TEXT_p','HTML_TEXT_textarea') AND len >10) "
                    + "OR (len >50)) AND wpage_id = "+pageID1);
            
            while(rs.next()){
                set1.add(rs.getString("META_DATA_VALUE"));
            }
            rs.close();
            
            rs = st.executeQuery("SELECT * FROM (SELECT meta_data_field,length(meta_data_value)len,meta_data_value,wpage_id FROM wpage_metadata) "
                    + "WHERE meta_data_field LIKE 'HTML_TEXT%' AND ((meta_data_field IN ('HTML_TEXT_li','HTML_TEXT_p','HTML_TEXT_textarea') AND len >10) "
                    + "OR (len >50)) AND wpage_id =  "+pageID2);
            
            while(rs.next()){
                set2.add(rs.getString("META_DATA_VALUE"));
            }
            rs.close();
            
            float jDist = getJaccardDistance(set1,set2);
            
            rs = st.executeQuery("SELECT * FROM WPAGE_JDIST WHERE WPAGE1_ID = "+pageID1+" AND WPAGE2_ID = "+pageID2
                    +" AND JDIST_TYPE ='HTML_TEXT'");
            
            if(rs.next()){
                String insertStatement = "UPDATE WPAGE_JDIST SET JDIST_VALUE = ? WHERE WPAGE1_ID=? AND "
                        + "WPAGE2_ID=? AND JDIST_TYPE=? ";
                pst = dbCon.prepareStatement(insertStatement);
                pst.setLong(2, pageID1);
                pst.setLong(3, pageID2);
                pst.setFloat(1, jDist);
                pst.setString(4, "HTML_TEXT");
                pst.executeUpdate();
            }
            else{
                String insertStatement = "INSERT INTO WPAGE_JDIST(WPAGE1_ID,WPAGE2_ID,JDIST_VALUE,JDIST_TYPE) "
                    + "VALUES(?,?,?,?)";
                pst = dbCon.prepareStatement(insertStatement);
                pst.setLong(1, pageID1);
                pst.setLong(2, pageID2);
                pst.setFloat(3, jDist);
                pst.setString(4, "HTML_TEXT");
                pst.executeUpdate();
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
    
    /*
     * Method to store the Jdist vaue between the HTML tags found in the source code
     * of two pages with id pageID1 and pageID2.
     * 
     * Comparisson is based on the concatenation of tagname and count of the tag in the html source.
     */
    public boolean storeHTagJDist(Connection dbCon,long pageID1, long pageID2){
        Set set1 = new HashSet();
        Set set2 = new HashSet();
        ResultSet rs = null;
        Statement st = null;
        PreparedStatement pst = null;
        
        //If both pages are the same, return false and dont store any values
        if(pageID1 == pageID2)
            return false;
        
        //Always keep pageID1 as the smaller value to remove duplicates
        if(pageID1 > pageID2){
            long tmp = pageID1;
            pageID1 = pageID2;
            pageID2 = tmp;
        }
        
        try{
            st = dbCon.createStatement();
            rs = st.executeQuery("SELECT * FROM wpage_metadata WHERE meta_data_field LIKE 'TAG_COUNT%' AND wpage_id = "+pageID1);
            
            while(rs.next()){
                set1.add("<"+rs.getString("META_DATA_FIELD").substring(10)+">"+rs.getString("META_DATA_VALUE"));
            }
            rs.close();
            
            rs = st.executeQuery("SELECT * FROM wpage_metadata WHERE meta_data_field LIKE 'TAG_COUNT%' AND wpage_id = "+pageID2);
            
            while(rs.next()){
                set2.add("<"+rs.getString("META_DATA_FIELD").substring(10)+">"+rs.getString("META_DATA_VALUE"));
            }
            rs.close();
            
            float jDist = getJaccardDistance(set1,set2);
            
            rs = st.executeQuery("SELECT * FROM WPAGE_JDIST WHERE WPAGE1_ID = "+pageID1+" AND WPAGE2_ID = "+pageID2
                    +" AND JDIST_TYPE ='HTML_TAG_COUNT'");
            
            if(rs.next()){
                String insertStatement = "UPDATE WPAGE_JDIST SET JDIST_VALUE = ? WHERE WPAGE1_ID=? AND "
                        + "WPAGE2_ID=? AND JDIST_TYPE=? ";
                pst = dbCon.prepareStatement(insertStatement);
                pst.setLong(2, pageID1);
                pst.setLong(3, pageID2);
                pst.setFloat(1, jDist);
                pst.setString(4, "HTML_TAG_COUNT");
                pst.executeUpdate();
            }
            else{
                String insertStatement = "INSERT INTO WPAGE_JDIST(WPAGE1_ID,WPAGE2_ID,JDIST_VALUE,JDIST_TYPE) "
                    + "VALUES(?,?,?,?)";
                pst = dbCon.prepareStatement(insertStatement);
                pst.setLong(1, pageID1);
                pst.setLong(2, pageID2);
                pst.setFloat(3, jDist);
                pst.setString(4, "HTML_TAG_COUNT");
                pst.executeUpdate();
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
    
    /*
     * Method to store the Jdist vaue between the file names used by the html source code
     * of two pages with id pageID1 and pageID2.
     */
    public boolean storeFileJDist(Connection dbCon,long pageID1, long pageID2){
        Set set1 = new HashSet();
        Set set2 = new HashSet();
        ResultSet rs = null;
        Statement st = null;
        PreparedStatement pst = null;
        
        //If both pages are the same, return false and dont store any values
        if(pageID1 == pageID2)
            return false;
        
        //Always keep pageID1 as the smaller value to remove duplicates
        if(pageID1 > pageID2){
            long tmp = pageID1;
            pageID1 = pageID2;
            pageID2 = tmp;
        }
        
        try{
            st = dbCon.createStatement();
            rs = st.executeQuery("SELECT * FROM wpage_metadata WHERE meta_data_field LIKE 'FILE_NAME' AND wpage_id = "+pageID1);
            
            while(rs.next()){
                set1.add(rs.getString("META_DATA_VALUE"));
            }
            rs.close();
            
            rs = st.executeQuery("SELECT * FROM wpage_metadata WHERE meta_data_field LIKE 'FILE_NAME' AND wpage_id = "+pageID2);
            
            while(rs.next()){
                set2.add(rs.getString("META_DATA_VALUE"));
            }
            rs.close();
            
            float jDist = getJaccardDistance(set1,set2);
            
            rs = st.executeQuery("SELECT * FROM WPAGE_JDIST WHERE WPAGE1_ID = "+pageID1+" AND WPAGE2_ID = "+pageID2
                    +" AND JDIST_TYPE ='FILE_NAME'");
            
            if(rs.next()){
                String insertStatement = "UPDATE WPAGE_JDIST SET JDIST_VALUE = ? WHERE WPAGE1_ID=? AND "
                        + "WPAGE2_ID=? AND JDIST_TYPE=? ";
                pst = dbCon.prepareStatement(insertStatement);
                pst.setLong(2, pageID1);
                pst.setLong(3, pageID2);
                pst.setFloat(1, jDist);
                pst.setString(4, "FILE_NAME");
                pst.executeUpdate();
            }
            else{
                String insertStatement = "INSERT INTO WPAGE_JDIST(WPAGE1_ID,WPAGE2_ID,JDIST_VALUE,JDIST_TYPE) "
                    + "VALUES(?,?,?,?)";
                pst = dbCon.prepareStatement(insertStatement);
                pst.setLong(1, pageID1);
                pst.setLong(2, pageID2);
                pst.setFloat(3, jDist);
                pst.setString(4, "FILE_NAME");
                pst.executeUpdate();
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
    
    /*
     * Method used to update jDist values for all pairs of websites in the wpage table for the various parameters.
     * 
     * toUpdate - parameter used to specify which type of parameter should be used to compute jDist
     * xxx1 - builtwith
     * xx1x - text from html
     * x1xx - tags and count from html
     * 1xxx - file names from html
     */
    public boolean updateAllJDist(Connection dbCon, long toUpdate){
        ResultSet rs = null;
        Statement st = null;
        List<Long> pageIDList = new ArrayList<>();
        
        try{
            st = dbCon.createStatement();
            rs = st.executeQuery("SELECT * FROM wpage WHERE wpage_available = 'Y'");
            while(rs.next()){
                pageIDList.add(rs.getLong("WPAGE_ID"));
            }
            
            for(int i=0;i<pageIDList.size();i++){
                for(int j=i+1;j<pageIDList.size();j++){
                    if(toUpdate%10 == 1)
                        storeBuiltwithJDist(dbCon,pageIDList.get(i),pageIDList.get(j));
                    if((toUpdate/10)%10 == 1)
                        storeHTextJDist(dbCon,pageIDList.get(i),pageIDList.get(j));
                    if((toUpdate/100)%10 == 1)
                        storeHTagJDist(dbCon,pageIDList.get(i),pageIDList.get(j));
                    if((toUpdate/1000)%10 == 1)
                        storeFileJDist(dbCon,pageIDList.get(i),pageIDList.get(j));
                    if((toUpdate/10000)%10 == 1)
                        storeVocabJDist(dbCon,pageIDList.get(i),pageIDList.get(j));
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
            }catch(Exception ignore){}
        }
        
        return true;
    }
    
    /*
     * Method used to update jDist values for a newly added website with WPageId provided.
     * 
     * toUpdate - parameter used to specify which type of parameter should be used to compute jDist
     * xxx1 - builtwith
     * xx1x - text from html
     * x1xx - tags and count from html
     * 1xxx - file names from html
     */
    public boolean updateAllJDistSinglePage(Connection dbCon, long toUpdate, long wPageID){
        ResultSet rs = null;
        Statement st = null;
        List<Long> pageIDList = new ArrayList<>();
        
        try{
            st = dbCon.createStatement();
            rs = st.executeQuery("SELECT * FROM wpage WHERE wpage_available = 'Y'");
            while(rs.next()){
                pageIDList.add(rs.getLong("WPAGE_ID"));
            }
            
            for(int i=0;i<pageIDList.size();i++){
                if(toUpdate%10 == 1)
                    storeBuiltwithJDist(dbCon,pageIDList.get(i),wPageID);
                if((toUpdate/10)%10 == 1)
                    storeHTextJDist(dbCon,pageIDList.get(i),wPageID);
                if((toUpdate/100)%10 == 1)
                    storeHTagJDist(dbCon,pageIDList.get(i),wPageID);
                if((toUpdate/1000)%10 == 1)
                    storeFileJDist(dbCon,pageIDList.get(i),wPageID);
                if((toUpdate/10000)%10 == 1)
                    storeVocabJDist(dbCon,pageIDList.get(i),wPageID);
            }
        }
        catch(SQLException ex){
            Logger.getLogger(DataProcessor.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        finally{
            try{
                if(rs!=null)
                    rs.close();
                if(st!=null)
                    st.close();
            }catch(Exception ignore){}
        }
        
        return true;
    }
    
    /*
     * Method used to store the Jdist value between the vacabulary dictionaries
     * of two pages with id pageID1 and pageID2
     */
    public boolean storeVocabJDist(Connection dbCon,long pageID1, long pageID2){
        Set set1 = new HashSet();
        Set set2 = new HashSet();
        ResultSet rs = null;
        Statement st = null;
        PreparedStatement pst = null;
        
        //If both pages are the same, return false and dont store any values
        if(pageID1 == pageID2)
            return false;
        
        //Always keep pageID1 as the smaller value to remove duplicates
        if(pageID1 > pageID2){
            long tmp = pageID1;
            pageID1 = pageID2;
            pageID2 = tmp;
        }
        
        try{
            st = dbCon.createStatement();
            rs = st.executeQuery("SELECT * FROM (SELECT meta_data_field,length(meta_data_value)len,meta_data_value,wpage_id FROM wpage_metadata) "
                    + "WHERE meta_data_field LIKE 'HTML_TEXT%' AND ((meta_data_field IN ('HTML_TEXT_li','HTML_TEXT_p','HTML_TEXT_textarea') AND len >2) "
                    + "OR (len >2)) AND wpage_id = "+pageID1);
            
            while(rs.next()){
                set1.addAll(getAllStrings(rs.getString("META_DATA_VALUE")));
            }
            rs.close();
            
            rs = st.executeQuery("SELECT * FROM (SELECT meta_data_field,length(meta_data_value)len,meta_data_value,wpage_id FROM wpage_metadata) "
                    + "WHERE meta_data_field LIKE 'HTML_TEXT%' AND ((meta_data_field IN ('HTML_TEXT_li','HTML_TEXT_p','HTML_TEXT_textarea') AND len >2) "
                    + "OR (len >2)) AND wpage_id =  "+pageID2);
            
            while(rs.next()){
                set2.addAll(getAllStrings(rs.getString("META_DATA_VALUE")));
            }
            rs.close();
            
            float jDist = getJaccardDistance(set1,set2);
            
            rs = st.executeQuery("SELECT * FROM WPAGE_JDIST WHERE WPAGE1_ID = "+pageID1+" AND WPAGE2_ID = "+pageID2
                    +" AND JDIST_TYPE ='HTML_VOCAB'");
            
            if(rs.next()){
                String updateStatement = "UPDATE WPAGE_JDIST SET JDIST_VALUE = ? WHERE WPAGE1_ID=? AND "
                        + "WPAGE2_ID=? AND JDIST_TYPE=? ";
                pst = dbCon.prepareStatement(updateStatement);
                pst.setLong(2, pageID1);
                pst.setLong(3, pageID2);
                pst.setFloat(1, jDist);
                pst.setString(4, "HTML_VOCAB");
                pst.executeUpdate();
            }
            else{
                String insertStatement = "INSERT INTO WPAGE_JDIST(WPAGE1_ID,WPAGE2_ID,JDIST_VALUE,JDIST_TYPE) "
                    + "VALUES(?,?,?,?)";
                pst = dbCon.prepareStatement(insertStatement);
                pst.setLong(1, pageID1);
                pst.setLong(2, pageID2);
                pst.setFloat(3, jDist);
                pst.setString(4, "HTML_VOCAB");
                pst.executeUpdate();
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
    
    /*
     * Given a string, returns a set containing the unique words in that string excluding common words
     */
    private static Set<String> getAllStrings(String str){
        Set<String> ret = new HashSet<>();
        //Remove all braces before splitting the strings
        /*str = str.replace(",", " ");
        str = str.replace("(", "");
        str = str.replace(")", "");
        str = str.replace("[", "");
        str = str.replace("]", "");
        str = str.replace("{", "");
        str = str.replace("}", "");*/
        str = str.replace(",", " ");
        str = str.replace("(", " ");
        str = str.replace(")", " ");
        str = str.replace("[", " ");
        str = str.replace("]", " ");
        str = str.replace("{", " ");
        str = str.replace("}", " ");
        str = str.replace(".", " ");
        str = str.replace("?", " ");
        str = str.replace("!", " ");
        str = str.replace(":", " ");
        str = str.replace(";", " ");
        str = str.replace("\'", " ");
        str = str.replace("\"", " ");
        str = str.replace("\\", " ");
        str = str.replace("‘", " ");
        
        //Split each string by space and trim before you add
        for(String eachStr:str.split(" ")){
            eachStr = eachStr.trim().toLowerCase();
            if(!commonExclStrings.contains(eachStr) && !isNumeric(eachStr))
                ret.add(eachStr);
        }
        
        return ret;
    }
    
    /*
     * Given a string, the methods returns true if it is a number and false otherwise.
     */
    private static boolean isNumeric(String str){
        try{
            Double.parseDouble(str);
        }
        catch(NumberFormatException nfe){  
            return false;
        }  
        return true;
    }
}