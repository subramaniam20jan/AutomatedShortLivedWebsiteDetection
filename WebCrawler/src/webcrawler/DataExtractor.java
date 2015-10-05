/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package webcrawler;

import java.io.File;
import java.sql.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

/**
 *
 * @author Subu
 * 
 * The class has various methods that enable the extraction of metadata and data used for
 * analysis of websites in the db.
 */
public class DataExtractor {
    
    /*
     * Method used to extract all webpages in WPAGE and extract the tag counts and store them in the database
     */
    public boolean storeWpageTags(Connection dbCon){
        ResultSet rs = null;
        Statement st = null;
        PreparedStatement pst=null;
        
        try{
            st = dbCon.createStatement();
            rs = st.executeQuery("SELECT * FROM wpage WHERE wpage_id not in("
                    + "SELECT wpage.wpage_id FROM WPAGE,wpage_metadata wmeta WHERE WPAGE_AVAILABLE = 'Y' and wpage.wpage_id = wmeta.wpage_id "
                    + "and wmeta.meta_data_field like 'TAG%')"
                    + "AND WPAGE_AVAILABLE = 'Y'");
            
            while(rs.next()){
                Map<String,Integer> tags = extractTagsFromHTML(rs.getString("WPAGE_CONTENT_HTML"));
                
                Set<String> tagNames = tags.keySet();
                for(String tagName:tagNames){
                    int tagCount = tags.get(tagName);
                    
                    String insertStatement = "INSERT INTO WPAGE_METADATA"
                            + "(META_DATA_ID,WPAGE_ID,META_DATA_FIELD,META_DATA_VALUE,RECORD_UPDATED) VALUES"
                            + "(META_DATA_SEQUENCE.NEXTVAL,?,?,?,?)";
                    pst = dbCon.prepareStatement(insertStatement);
                    pst.setLong(1, rs.getLong("WPAGE_ID"));
                    pst.setString(2, "TAG_COUNT_"+tagName);
                    pst.setString(3, ""+tagCount);
                    pst.setString(4, "Y");
                    pst.executeUpdate();
                    pst.close();
                }
            }
        }
        catch(SQLException ex){
            Logger.getLogger(WebCrawler.class.getName()).log(Level.SEVERE, null, ex);
        }
        finally{
            try{
                if(st!=null)
                    st.close();
                if(rs!=null)
                    rs.close();
                if(pst!=null)
                    pst.close();
            }catch(Exception ignore){}
        }
        
        return true;
    }
    
    /*
     * Method takes an html source and returns a map of tag name to number of occurences
     * of the tag in the html source code.
     */
    private Map<String,Integer> extractTagsFromHTML(String htmlSource){        
        Map<String,Integer> ret = new HashMap<>();
        
        Document htmlDoc = Jsoup.parse(htmlSource);
        Elements elements = htmlDoc.getAllElements();
        for(Element element :elements ){
            if(!element.nodeName().equals("#document"))
                ret = addElement(element.nodeName(),ret);
        }
        
        return ret;
    }
    
    /*
     * Method takes a map and a string and adds an occurence to the string in the map
     */
    private static Map<String,Integer> addElement(String node, Map<String,Integer> map){
        
        if(map.containsKey(node))
            map.put(node, map.get(node)+1);
        else
            map.put(node, 1);
        
        return map;
    }
    
    /*
     * Method takes a db connection argument, queries the db for all eligible rows to extract text from and
     * extract all text phrases from the html source code which is stored in the wpage_metadata table.
     */
    public boolean storeWpageText(Connection dbCon,boolean updateAll){
        ResultSet rs = null;
        Statement st = null;
        ResultSet rs2 = null;
        PreparedStatement pst=null;
        Map<String,String> textList = null;
        
        try{
            st = dbCon.createStatement();
            if(updateAll)
                rs = st.executeQuery("SELECT * FROM wpage WHERE WPAGE_AVAILABLE = 'Y'");
            else
                rs = st.executeQuery("SELECT * FROM wpage WHERE wpage_id not in("
                        + "SELECT wpage.wpage_id FROM WPAGE,wpage_metadata wmeta WHERE WPAGE_AVAILABLE = 'Y' AND wpage.wpage_id = wmeta.wpage_id "
                        + "AND wmeta.meta_data_field LIKE 'HTML_TEXT%')"
                        + "AND WPAGE_AVAILABLE = 'Y'");
            
            while(rs.next()){
                textList = extractTextFromHTML(rs.getString("WPAGE_CONTENT_HTML"));
                
                Set<String> textListPlain = textList.keySet();
                for(String text:textListPlain){
                    String queryStatement ="SELECT * FROM WPAGE_METADATA WHERE META_DATA_FIELD=? AND "
                            + "META_DATA_VALUE = ? AND WPAGE_ID = ?";
                    if(pst != null && !pst.isClosed())
                            pst.close();
                    if(rs2 != null && !rs2.isClosed())
                        rs2.close();
                    pst = dbCon.prepareStatement(queryStatement);
                    pst.setString(1, "HTML_TEXT_"+textList.get(text));
                    if(text.length()>=1000)
                        pst.setString(2, "NON EXISTANT TEXT");
                    else
                        pst.setString(2, text);
                    pst.setLong(3, rs.getLong("WPAGE_ID"));
                    rs2 = pst.executeQuery();
                    
                    if(!rs2.next()){
                        String insertStatement = "INSERT INTO WPAGE_METADATA"
                            + "(META_DATA_ID,WPAGE_ID,META_DATA_FIELD,META_DATA_VALUE,RECORD_UPDATED) VALUES"
                            + "(META_DATA_SEQUENCE.NEXTVAL,?,?,?,?)";
                        if(pst!= null && !pst.isClosed())
                            pst.close();
                        pst = dbCon.prepareStatement(insertStatement);
                        pst.setLong(1, rs.getLong("WPAGE_ID"));
                        pst.setString(2, "HTML_TEXT_"+textList.get(text));
                        if(text.length()<1000)
                            pst.setString(3, text);
                        else
                            pst.setString(3,"RAW_TEXT");
                        pst.setString(4, "Y");
                        pst.executeUpdate();
                        pst.close();

                        if(text.length()>=1000)
                        {
                            insertStatement ="INSERT INTO RAW_METADATA (META_DATA_ID,RAW_DATA) values"
                                    + "(META_DATA_SEQUENCE.CURRVAL,?)";
                            pst = dbCon.prepareStatement(insertStatement);
                            pst.setString(1, text);
                            pst.executeUpdate();
                            pst.close();
                        }
                    }
                }
            }
        }
        catch(SQLException ex){
            Logger.getLogger(WebCrawler.class.getName()).log(Level.SEVERE, null, ex);
        }
        finally{
            try{
                if(st!=null && !st.isClosed())
                    st.close();
                if(rs!=null && !rs.isClosed())
                    rs.close();
                if(rs2!=null && !rs2.isClosed())
                    rs2.close();
                if(pst!=null && !pst.isClosed())
                    pst.close();
            }catch(Exception ignore){}
        }
        
        return true;
    }
    
    /*
     * Method takes an html source and returns a list of text strings from the webpage
     */
    private Map<String,String> extractTextFromHTML(String htmlSource){        
        Map<String,String> ret = new HashMap<>();
        
        Document htmlDoc = Jsoup.parse(htmlSource);
        Elements elements = htmlDoc.getAllElements();
        for(Element element :elements ){
            if(!element.nodeName().equals("#document"))
                /*if(element.nodeName().equals("h1")||element.nodeName().equals("h2")||element.nodeName().equals("h3")
                        ||element.nodeName().equals("h4")||element.nodeName().equals("h5")||element.nodeName().equals("header")
                        ||element.nodeName().equals("p")||element.nodeName().equals("textarea")||element.nodeName().equals("title")
                        ||element.nodeName().equals("footer")||element.nodeName().equals("big")||element.nodeName().equals("li")
                        ||element.nodeName().equals("label")||element.nodeName().equals("td")||element.nodeName().equals("tr"))*/
                    if(!element.text().trim().isEmpty())
                        ret.put(element.text(),element.nodeName());
        }
        
        return ret;
    }
    
    /*
     * Given a path to a folder and a db connection, attempts to obtain the file names 
     * for the webpages in the db from the corresponding folder and pushes them into the db specified.
     */
    public boolean storeFNamesFromPath(String path, Connection dbCon){
        
        ResultSet rs = null;
        Statement st = null;
        PreparedStatement pst=null;
        
        try{
            st = dbCon.createStatement();
            rs = st.executeQuery("SELECT * FROM wpage WHERE wpage_id not in("
                    + "SELECT wpage.wpage_id FROM WPAGE,wpage_metadata wmeta WHERE WPAGE_AVAILABLE = 'Y' and wpage.wpage_id = wmeta.wpage_id "
                    + "and wmeta.meta_data_field like 'FILE_NAME')"
                    + "AND WPAGE_AVAILABLE = 'Y'");
            
            while(rs.next()){
                String folderName = rs.getString("WPAGE_NAME").substring(7).trim();
                Set<String> fleList = fileTraverse(new File(path+folderName).listFiles());;
                
                for(String fileName:fleList){
                    
                    String insertStatement = "INSERT INTO WPAGE_METADATA"
                            + "(META_DATA_ID,WPAGE_ID,META_DATA_FIELD,META_DATA_VALUE,RECORD_UPDATED) VALUES"
                            + "(META_DATA_SEQUENCE.NEXTVAL,?,?,?,?)";
                    pst = dbCon.prepareStatement(insertStatement);
                    pst.setLong(1, rs.getLong("WPAGE_ID"));
                    pst.setString(2, "FILE_NAME");
                    if(fileName.length()<1000)
                        pst.setString(3, fileName);
                    else
                        pst.setString(3,"RAW_TEXT");
                    pst.setString(4, "Y");
                    pst.executeUpdate();
                    pst.close();
                    
                    if(fileName.length()>=1000)
                    {
                        insertStatement ="INSERT INTO RAW_METADATA (META_DATA_ID,RAW_DATA) values"
                                + "(META_DATA_SEQUENCE.CURRVAL,?)";
                        pst = dbCon.prepareStatement(insertStatement);
                        pst.setString(1, fileName);
                        pst.executeUpdate();
                        pst.close();
                    }
                }
            }
        }
        catch(SQLException ex){
            Logger.getLogger(WebCrawler.class.getName()).log(Level.SEVERE, null, ex);
        }
        finally{
            try{
                if(st!=null)
                    st.close();
                if(rs!=null)
                    rs.close();
                if(pst!=null)
                    pst.close();
            }catch(Exception ignore){}
        }
        
        //fileTraverse(new File(path).listFiles());
        return true;
    }
    
    /*
     * static recursive method to iterate through a file array and return all the filenames under that path
     */
    private static Set<String> fileTraverse(File[] files){
        Set<String> ret = new HashSet<>();
        
        if(files == null)
            return ret;
        
        for(File file:files ){
            if(file.isDirectory())
                ret.addAll(fileTraverse(file.listFiles()));
            else
                ret.add(file.getName());
        }
        
        return ret;
    }
    
    /*
     * Method to retrieve builtwith data for all available webpages and stores them in the database 
     * connection provided.
     */
    public boolean updateMissingBuilthWith(String destPath,Connection dbCon) throws InterruptedException{
        
        ResultSet rs = null;
        Statement st = null;
        WebCrawler wc = new WebCrawler();
        
        try{
            st = dbCon.createStatement();
            rs = st.executeQuery("select * from wpage where wpage_id not in(select wpage_id from builtwith_key) "
                    + "and wpage_available = 'Y'");
            
            while(rs.next()){
                String name = rs.getString("WPAGE_NAME").substring(7).trim();
                Thread.sleep(2000);
                wc.saveBuiltWith(name, destPath+"\\"+name+"\\",dbCon,true);
            }
            
        }
        catch(SQLException ex){
            Logger.getLogger(WebCrawler.class.getName()).log(Level.SEVERE, null, ex);
        }
        finally{
            wc.closeWebCrawler();
            try{
                if(st!=null)
                    st.close();
                if(rs!=null)
                    rs.close();
            }catch(Exception ignore){}
        }
        
        return true;
    }
}