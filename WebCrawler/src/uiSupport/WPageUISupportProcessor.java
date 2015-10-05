/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package uiSupport;


import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import webcrawler.DataExtractor;
import webcrawler.DataProcessor;
import webcrawler.WebCrawler;

/**
 *
 * @author Subu
 * 
 * This class is meant to be associated with one page identified by the wpageID value.
 * 
 * When the class is instantiated, this information is obtained either by creating a new page
 * entry in the db or by retrieving the existing page id.
 * 
 * Once instantiated, it provides methods that help provide information and 
 * operations that can be performed on the page from the UI.
 */
public class WPageUISupportProcessor {
    
    long wPageID=0;
    Connection dbCon;
    String destLocation = "";
    BufferedWriter log = null;
    WebCrawler wc = null;
    DataExtractor de;
    DataProcessor dp;
    
    /*
     * Directly sets the WPageID value for the class assuming it already exists in the db
     */
    public WPageUISupportProcessor(Connection dbCon, Long WPageID){
        this.wPageID = WPageID;
        this.dbCon = dbCon;
    }
    
    /*
     * Constructor used to instantiate the object with a url string.
     * 
     * Will create the new page if it is not already in the db and process all JDist values.
     * 
     * Will update the WPageID value for the class.
     */
    public WPageUISupportProcessor(Connection dbCon, String url, String destLocation){
        this.dbCon = dbCon;
        this.destLocation = destLocation;
        
        PreparedStatement pst=null;
        ResultSet rs=null;
        
        try{
            FileOutputStream fLog;
            fLog = new FileOutputStream(destLocation+"log.txt");
            log = new BufferedWriter(new OutputStreamWriter(fLog));
            
            String query = "SELECT * FROM WPAGE WHERE WPAGE_NAME = ?";
            pst = dbCon.prepareStatement(query);
            pst.setString(1, "http://"+url);
            rs = pst.executeQuery();
            
            if(!rs.next()){
                wc = new WebCrawler();
                dp = new DataProcessor(dbCon);
                de = new DataExtractor();
                this.wPageID = addNewWPageComplete(url);
            }
            else
                this.wPageID=rs.getLong("WPAGE_ID");
        }
        catch(SQLException ex){
            
        }
        catch (FileNotFoundException ex) {
                Logger.getLogger(WPageUISupportProcessor.class.getName()).log(Level.SEVERE, null, ex);
            }
        finally{
            try{
                if(rs!=null && !pst.isClosed())
                    rs.close();
                if(pst!=null && !pst.isClosed())
                    pst.close();
            }catch(Exception ignore){}
        }
    }
    
    /*
     * Method used to download the webpage content, extract all relevant data and calculate Jdist for the 
     * newly added webpage based on the url provided.
     */
    protected final long addNewWPageComplete(String pageName){
        long ret=0;
        PreparedStatement pst = null;
        ResultSet rs = null;
        
        pageName = pageName.trim();
        if(!(Character.isLetter(pageName.charAt(0))||Character.isDigit(pageName.charAt(0)))){
            pageName = pageName.substring(1);
        }
        if(pageName.isEmpty()){
            return -1l;
        }
        new File(destLocation+pageName).mkdir();
        String url = "http://"+pageName;
        try {
            //Store the webpage after downloading it
            if(!wc.storeWebPage(url,destLocation+"\\"+pageName+"\\",log,dbCon,false)){
                log.write("\nProblem Accessing website "+url+" "+new Date().toString());
                return -1l;
            }
            else{
                //Retrieve the wpageID of the newly created page
                String query = "SELECT * FROM WPAGE WHERE WPAGE_NAME = ?";
                pst = dbCon.prepareStatement(query);
                pst.setString(1, url);
                rs = pst.executeQuery();
                if(rs.next())
                    this.wPageID = rs.getLong("WPAGE_ID");
                else
                    return -1l;
                
                log.write("\nSuccessfully saved "+url+" "+new Date().toString());
                //Store builtwith data if not already available
                wc.saveBuiltWith(pageName, destLocation+"\\"+pageName+"\\",dbCon,false);
                //Extract metadata from the webpage information if not already available
                de.storeWpageTags(dbCon);
                de.storeWpageText(dbCon, false);
                de.storeFNamesFromPath(destLocation, dbCon);
                
                //Calculate JDist for all the parameters extracted
                dp.updateAllJDistSinglePage(dbCon, 11111, wPageID);
            }
        }
        catch (IOException | SQLException ex) {
            Logger.getLogger(WPageUISupportProcessor.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return wPageID;
    }
    
    /*
     * Method returns the unrated rank of the page
     */
    public long getUnRatedRank(){
        long ret=0;
        PreparedStatement pst = null;
        ResultSet rs = null;
        
        try{
            String query = "SELECT * FROM wpage_ur_jdist_vocab_min_view WHERE wpage_id =?";
            pst = dbCon.prepareStatement(query);
            pst.setLong(1, wPageID);
            rs = pst.executeQuery();
            if(rs.next())
                ret = rs.getLong("WPG_RANK");
        }
        catch(SQLException ex){
            Logger.getLogger(WPageUISupportProcessor.class.getName()).log(Level.SEVERE, null, ex);
        }
        finally{
            try{
                if(pst!=null && !pst.isClosed())
                    pst.close();
                if(rs!=null && !rs.isClosed())
                    rs.close();
            }catch(SQLException ignore){}
        }
        
        return ret;
    }
    
    /*
     * Methods returns the position of the wpage in the global ranking by vocab jdist.
     */
    public long getGlobalRank(){
        long ret=0;
        PreparedStatement pst = null;
        ResultSet rs = null;
        
        try{
            String query = "SELECT * FROM wpage_jdist_vocab_min_view WHERE wpage_id =?";
            pst = dbCon.prepareStatement(query);
            pst.setLong(1, wPageID);
            rs = pst.executeQuery();
            if(rs.next())
                ret = rs.getLong("WPG_RANK");
        }
        catch(SQLException ex){
            Logger.getLogger(WPageUISupportProcessor.class.getName()).log(Level.SEVERE, null, ex);
        }
        finally{
            try{
                if(pst!=null && !pst.isClosed())
                    pst.close();
                if(rs!=null && !rs.isClosed())
                    rs.close();
            }catch(SQLException ignore){}
        }
        
        return ret;
    }
    
    /*
     * Method returns the total number of wpages in the db.
     */
    public long getTotalPages(){
        long ret=0;
        PreparedStatement pst = null;
        ResultSet rs = null;
        
        try{
            String query = "SELECT count(1) cnt FROM wpage_jdist_vocab_min_view";
            pst = dbCon.prepareStatement(query);
            rs = pst.executeQuery();
            if(rs.next())
                ret = rs.getLong("cnt");
        }
        catch(SQLException ex){
            Logger.getLogger(WPageUISupportProcessor.class.getName()).log(Level.SEVERE, null, ex);
        }
        finally{
            try{
                if(pst!=null && !pst.isClosed())
                    pst.close();
                if(rs!=null && !rs.isClosed())
                    rs.close();
            }catch(SQLException ignore){}
        }
        
        return ret;
    }
    
    /*
     * Gets the total number of pages that are unrated by the user.
     */
    public long getTotalUnratedPages(){
        long ret=0;
        PreparedStatement pst = null;
        ResultSet rs = null;
        
        try{
            String query = "SELECT count(1) cnt FROM wpage_jdist_vocab_min_view WHERE wpage_upvote is null";
            pst = dbCon.prepareStatement(query);
            rs = pst.executeQuery();
            if(rs.next())
                ret = rs.getLong("cnt");
        }
        catch(SQLException ex){
            Logger.getLogger(WPageUISupportProcessor.class.getName()).log(Level.SEVERE, null, ex);
        }
        finally{
            try{
                if(pst!=null && !pst.isClosed())
                    pst.close();
                if(rs!=null && !rs.isClosed())
                    rs.close();
            }catch(SQLException ignore){}
        }
        
        return ret;
    }
    
    /*
     * Method returns true or false depending on if a wpage is set.
     * 
     * Ideally expected to always return true if the object is usable.
     */
    public boolean isPageLoaded(){
        if(wPageID == 0 || wPageID == -1l)
            return false;
        else
            return true;
    }
    
    /*
     * Returns the html source of the wpage loaded.
     */
    public String getPageSource(){
        String ret = "";
        
        PreparedStatement pst = null;
        ResultSet rs = null;
        
        try{
            String query = "SELECT * FROM wpage WHERE wpage_id = ?";
            pst = dbCon.prepareStatement(query);
            pst.setLong(1, wPageID);
            rs = pst.executeQuery();
            if(rs.next())
                ret = rs.getString("WPAGE_CONTENT_HTML");
        }
        catch(SQLException ex){
            Logger.getLogger(WPageUISupportProcessor.class.getName()).log(Level.SEVERE, null, ex);
        }
        finally{
            try{
                if(pst!=null && !pst.isClosed())
                    pst.close();
                if(rs!=null && !rs.isClosed())
                    rs.close();
            }catch(SQLException ignore){}
        }
        
        return ret;
    }
    
    /*
     * Method used to upvote or downvote the loaded page.
     */
    public boolean votePage(boolean vote){
        
        PreparedStatement pst = null;
        
        try{
            String query = "UPDATE WPAGE SET WPAGE_UPVOTE = ? WHERE WPAGE_ID = ?";
            pst = dbCon.prepareStatement(query);
            if(vote)
                pst.setLong(1, 1);
            else
                pst.setLong(1, -1);
            pst.setLong(2, wPageID);
            pst.executeUpdate();
            dbCon.commit();
        }
        catch(SQLException ex){
            Logger.getLogger(WPageUISupportProcessor.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        finally{
            try{
                if(pst!=null && !pst.isClosed())
                    pst.close();
            }catch(SQLException ignore){}
        }
        
        return true;
    }
    
    /*
     * Method used to save the jdist of type jdistType as a csv file in the
     * path/file specified in the target parameter.
     * 
     * Returns true if the file save was successful, else false.
     */
    public boolean getJdistCSV(String jdistType,String target){
        PreparedStatement pst = null;
        ResultSet rs = null;
        
        try{
            pst = dbCon.prepareStatement("SELECT wpage1_id , wpage2_id, jdist_value*100 jdist_value"
                    + " FROM wpage_jdist_view WHERE (wpage1_id = ? OR wpage2_id = ?) AND jdist_type = ?");
            pst.setLong(1, wPageID);
            pst.setLong(2, wPageID);
            pst.setString(3, jdistType);
            rs = pst.executeQuery();
            
            PrintWriter fileWriter = new PrintWriter(target, "UTF-8");
            fileWriter.println("Source,Target,Type,Weight");
            while(rs.next()){
                fileWriter.println(rs.getLong("wpage1_id")+","+rs.getLong("wpage2_id")+",undirected,"+rs.getFloat("jdist_value"));
            }
            fileWriter.close();
        }
        catch(SQLException ex){
            Logger.getLogger(WPageUISupportProcessor.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        catch(FileNotFoundException ex){
            Logger.getLogger(WPageUISupportProcessor.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        catch(UnsupportedEncodingException ex){
            Logger.getLogger(WPageUISupportProcessor.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        finally{
            try{
                if(rs!=null && !rs.isClosed())
                    rs.close();
                if(pst!=null && !pst.isClosed())
                    pst.close();
            }catch(SQLException ignore){}
        }
        
        return true;
    }
    
    /*
     * Method returns a list of all jdist_type values in the database
     */
    public List<String> getJDistType (){
        List<String> ret = new ArrayList<>();
        ResultSet rs = null;
        Statement st = null;
        
        try{
            st = dbCon.createStatement();
            rs = st.executeQuery("SELECT * FROM jdist_type_view");
            while(rs.next())
                ret.add(rs.getString("JDIST_TYPE"));
        }
        catch(SQLException ex){
            Logger.getLogger(WPageUISupportProcessor.class.getName()).log(Level.SEVERE, null, ex);
        }
        finally{
            try{
                if(rs!=null && !rs.isClosed())
                    rs.close();
                if(st!=null && !st.isClosed())
                    st.close();
            }catch(SQLException ignore){}
        }
        return ret;
    }
    
    /*
     * Method returns the list of all pages in the database
     */
    public static List<String> getWPageNames(Connection con){
        List<String> ret = new ArrayList<>();
        
        ResultSet rs = null;
        Statement st = null;
        
        try{
            st = con.createStatement();
            rs = st.executeQuery("SELECT unique(wpage_name) WPAGE_NAME FROM wpage order by wpage_name");
            while(rs.next())
                ret.add(rs.getString("WPAGE_NAME").substring(7));
        }
        catch(SQLException ex){
            Logger.getLogger(WPageUISupportProcessor.class.getName()).log(Level.SEVERE, null, ex);
        }
        finally{
            try{
                if(rs!=null && !rs.isClosed())
                    rs.close();
                if(st!=null && !st.isClosed())
                    st.close();
            }catch(SQLException ignore){}
        }
        
        return ret;
    }
}