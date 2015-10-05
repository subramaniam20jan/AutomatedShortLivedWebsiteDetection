/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package webcrawler;

import com.gargoylesoftware.htmlunit.BrowserVersion;
import com.gargoylesoftware.htmlunit.StringWebResponse;
import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.WebResponse;
import com.gargoylesoftware.htmlunit.html.DomNode;
import com.gargoylesoftware.htmlunit.html.HTMLParser;
import com.gargoylesoftware.htmlunit.html.HtmlAnchor;
import com.gargoylesoftware.htmlunit.html.HtmlPage;
import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.UnknownHostException;
import java.sql.*;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.w3c.dom.Node;

/**
 *
 * @author Subu
 */
public class WebCrawler {

    //Webclient to access webpages
    private WebClient webClient;
    //private UserAgent ua;
    
    //Prod key
    //private String builtWithKey="194415f1-e595-4a0d-ae9c-836950e7dde2";
    //private String builtWithKey="8426d4aa-df0a-4577-ac31-869d0f093048";
    private String builtWithKey="5c14fcee-741b-4b63-abc3-f308bf40cc19";
    private String whoIsKey = "1ffc878be8bbbc4a6838401ef59d61ec";
    long lastWhoIsQuery;
    
    public WebCrawler()
    {
        //Turn off htmlunit logging
        java.util.logging.Logger.getLogger("com.gargoylesoftware").setLevel(Level.OFF); 
        System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.NoOpLog");
        
        webClient = new WebClient(BrowserVersion.FIREFOX_24);
        webClient.getOptions().setThrowExceptionOnScriptError(false);
        lastWhoIsQuery = 0;
        //ua = new UserAgent();
    }
    
    /*
     * Must be called at the end to close the webclient
     */
    public void closeWebCrawler()
    {
        webClient.closeAllWindows();
        webClient = null;
    }
    
    /*
     * Stores the content of the webpage specified in the url to the DB and the location.
     * Return true if successfull and false otherwise.
     * 
     * Added functionality to save WhoIs information by called saveWhoIs method.
     */
    public boolean storeWebPage(String url,String destLocation,BufferedWriter log, Connection con, boolean forceUpdate)throws IOException
    {
        log.flush();
        PreparedStatement pst=null;
        PreparedStatement pstQuery =null;
        ResultSet rs = null;
        Statement st = null;
        //Save the html content after execution of the code in the page
        try{
            
            con.setAutoCommit(true);
            st = con.createStatement();
            String query = "SELECT * FROM WPAGE WHERE WPAGE_AVAILABLE = 'Y' AND WPAGE_NAME =?";
            pstQuery = con.prepareStatement(query);
            pstQuery.setString(1, url);
            rs = pstQuery.executeQuery();
            
            if(!rs.next() || forceUpdate){
                webClient.getOptions().setThrowExceptionOnFailingStatusCode(false);
                HtmlPage page = webClient.getPage(url);
                webClient.getOptions().setThrowExceptionOnFailingStatusCode(true);
                if(page == null)
                    throw new PageNotFoundException(url);
                WebResponse response = page.getWebResponse();
                FileOutputStream fout = new FileOutputStream (destLocation+"data.html");
                BufferedWriter bout = new BufferedWriter(new OutputStreamWriter(fout));
                bout.write(response.getContentAsString());
                //Saves all contents of the webpage like a browser would
                File destFile = new File(destLocation+"datacomplete.html");
                destFile.delete();
                page.save(destFile);
                
                if(rs !=null && !rs.isClosed())
                    rs.close();
                rs = st.executeQuery("SELECT * FROM WPAGE WHERE WPAGE_NAME = '"+url+"'");
                //Insert the new row
                if(!rs.next()){
                    String insertStatement = "INSERT INTO WPAGE "+
                        "(WPAGE_ID,WPAGE_NAME,WPAGE_CONTENT_HTML,WPAGE_CREATE_DATE,WPAGE_LAST_UPDATED,WPAGE_AVAILABLE,RECORD_UPDATED) VALUES"+
                            "(WPAGE_SEQUENCE.NEXTVAL,?,?,SYSDATE,SYSDATE,?,?)";
                    pst = con.prepareStatement(insertStatement);
                    pst.setString(1, url);
                    pst.setString(2, response.getContentAsString());
                    pst.setString(3, "Y");
                    pst.setInt(4, 1);
                    pst.executeUpdate();
                    pst.close();
                }
                //Update the new row
                else{
                    long WPageId = rs.getLong("WPAGE_ID");
                    String insertStatement = "UPDATE WPAGE "+
                        "SET WPAGE_CONTENT_HTML=?, WPAGE_LAST_UPDATED = SYSDATE, RECORD_UPDATED = ? WPAGE_AVAILABLE = 'Y' WHERE "+
                            "WPAGE_ID = ?";
                    pst = con.prepareStatement(insertStatement);
                    pst.setString(1, response.getContentAsString());
                    pst.setLong(3, WPageId);
                    pst.setInt(2, 1);
                    pst.executeUpdate();
                    pst.close();
                }
                
                if(rs !=null && !rs.isClosed())
                    rs.close();
                rs = st.executeQuery("SELECT * FROM WPAGE WHERE WPAGE_NAME = '"+url+"'");
                //Call method to save whoIs information.
                if(rs.next())
                    if(!saveWhoIs(url, con, rs.getLong("WPAGE_ID")))
                        log.write("\n Problem saving WhoIs information for "+url);
                st.close();
            }
        }
        catch(UnknownHostException | PageNotFoundException | MalformedURLException e)
        {
            log.write("\nHost could not be accessed "+url);
            
            //Update in DB if the host is unresolvable
            try{
                ResultSet rsTmp = st.executeQuery("SELECT * FROM WPAGE WHERE WPAGE_NAME='"+url+"'");
                if(!rsTmp.next()){
                    String insertStatement = "INSERT INTO WPAGE(WPAGE_ID,WPAGE_NAME,WPAGE_CREATE_DATE,WPAGE_LAST_UPDATED,WPAGE_AVAILABLE,RECORD_UPDATED) VALUES"
                        + "(WPAGE_SEQUENCE.NEXTVAL,?,SYSDATE,SYSDATE,?,?)";
                    pst = con.prepareStatement(insertStatement);
                    pst.setString(1,url);
                    pst.setString(2, "N");
                    pst.setInt(3, 0);
                    pst.execute();
                    pst.close();
                }
                rsTmp.close();
            }
            catch(SQLException ex)
            {
                Logger.getLogger(WebCrawler.class.getName()).log(Level.SEVERE, null, ex);
            }
            return false;
        }
        catch(Exception e)
        {
            log.write("\nAn error occured while Chrome tried to access "+url+" "+e);
            log.write(e.getStackTrace().toString());
        }
        finally{
            try{
                if(pstQuery!=null)
                    pstQuery.close();
                if(st!=null)
                    st.close();
                if(rs!=null)
                    rs.close();
                if(pst!=null)
                    pst.close();
            }catch(Exception ignore){}
        }
        
        /*Save the entire webpage in a folder
        try{
            ua = new UserAgent();
            ua.visit(url);
            ua.download(url, new File(destLocation+"page.html"));
            ua.doc.saveCompleteWebPage(new File(destLocation+"page.html"));
            ua.close();
        }
        catch(JauntException j){
            log.write("\nAn error occured while User agent tried to access "+url+" "+j);
            System.err.println("Jaunt Exception "+j);
            return false;
        }
        catch(ConcurrentModificationException e)
        {
            log.write("\nAn error may have occured while User agent tried to access "+url+e.toString());
        }
        catch(Exception e)
        {
            log.write("\nAn error occured while User agent tried to access "+url+" "+e);
            return false;
        }*/
        return true;
    }
    
    /*
     * Method used to lookup WhoIs information and store it in the db.
     * 
     * Does not support forceUpdate mode!
     */
    public boolean saveWhoIs(String url, Connection con,long pageId)
    {
        String baseUrl = "http://api.whoapi.com/?domain="+url+"&r=whois&apikey="+whoIsKey;
        BufferedReader in=null;
        Statement st = null;
        ResultSet rs = null;
        try{
            //Update the whoIsInformation only if not already available.
            st = con.createStatement();
            rs = st.executeQuery("SELECT * FROM WPAGE_METADATA WHERE WPAGE_ID = "+pageId);
            
            if(rs.next()){
                return true;
            }
            
            //Check for the last WhoIs Query and wait till 60 seconds if needed
            long currentTime = new java.util.Date().getTime();
            if(currentTime-lastWhoIsQuery <60000){
                    Thread.sleep(60000-(currentTime-lastWhoIsQuery));
                    lastWhoIsQuery = new java.util.Date().getTime();
            }
            else if(lastWhoIsQuery == 0)
                lastWhoIsQuery = new java.util.Date().getTime();
            
            URL myURL = new URL(baseUrl);
            URLConnection myURLConnection = myURL.openConnection();
            myURLConnection.connect();
            in = new BufferedReader(new InputStreamReader(
                                    myURLConnection.getInputStream()));
            String jsonStr = in.readLine();
            
            if(!parseAndSaveWhoIs(jsonStr,con,pageId))
                return false;
        }
        catch(IOException | InterruptedException ex)
        {
            Logger.getLogger(WebCrawler.class.getName()).log(Level.SEVERE, null, ex);
            return false;
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
                in.close();
            }catch(SQLException | IOException ignore){}
        }
        
        return true;
    }
    /*
     * The method parses the provided jsonString and saves the useful whois 
     * information to the wpage_metadata table in the db specified.
     * 
     */
    protected boolean parseAndSaveWhoIs(String jsonString, Connection con, long pageId){
        
        JSONObject json;
        PreparedStatement pst = null;
        
        try {
            json = new JSONObject(jsonString);
            if(json.getInt("status")!=0)
                return false;
            
            JSONArray jsonContacts = json.getJSONArray("contacts");
            for(int i=0;i<jsonContacts.length();i++){
                String contactType = jsonContacts.getJSONObject(i).getString("type");
                String contactPhone = jsonContacts.getJSONObject(i).getString("phone");
                String contactEmail = jsonContacts.getJSONObject(i).getString("email");
                String contactCountry = jsonContacts.getJSONObject(i).getString("country");
                String contactAddress = jsonContacts.getJSONObject(i).getString("full_address");
                String contactOrganization = jsonContacts.getJSONObject(i).getString("organization");
                
                //Insert phone number
                String insertStatement = "INSERT INTO WPAGE_METADATA"
                        + "(META_DATA_ID,WPAGE_ID,META_DATA_FIELD,META_DATA_VALUE,RECORD_UPDATED) "
                        + " VALUES (META_DATA_SEQUENCE.NEXTVAL,?,?,?,?)";
                pst = con.prepareStatement(insertStatement);
                pst.setLong(1, pageId);
                pst.setString(2, contactType+"_PHONE");
                pst.setString(3, contactPhone);
                pst.setString(4, "Y");
                pst.executeUpdate();
                pst.close();
                
                //Insert email
                insertStatement = "INSERT INTO WPAGE_METADATA"
                        + "(META_DATA_ID,WPAGE_ID,META_DATA_FIELD,META_DATA_VALUE,RECORD_UPDATED) "
                        + " VALUES (META_DATA_SEQUENCE.NEXTVAL,?,?,?,?)";
                pst = con.prepareStatement(insertStatement);
                pst.setLong(1, pageId);
                pst.setString(2, contactType+"_EMAIL");
                pst.setString(3, contactEmail);
                pst.setString(4, "Y");
                pst.executeUpdate();
                pst.close();
                
                //Insert country
                insertStatement = "INSERT INTO WPAGE_METADATA"
                        + "(META_DATA_ID,WPAGE_ID,META_DATA_FIELD,META_DATA_VALUE,RECORD_UPDATED) "
                        + " VALUES (META_DATA_SEQUENCE.NEXTVAL,?,?,?,?)";
                pst = con.prepareStatement(insertStatement);
                pst.setLong(1, pageId);
                pst.setString(2, contactType+"_COUNTRY");
                pst.setString(3, contactCountry);
                pst.setString(4, "Y");
                pst.executeUpdate();
                pst.close();
                
                //Insert address
                insertStatement = "INSERT INTO WPAGE_METADATA"
                        + "(META_DATA_ID,WPAGE_ID,META_DATA_FIELD,META_DATA_VALUE,RECORD_UPDATED) "
                        + " VALUES (META_DATA_SEQUENCE.NEXTVAL,?,?,?,?)";
                pst = con.prepareStatement(insertStatement);
                pst.setLong(1, pageId);
                pst.setString(2, contactType+"_ADDRESS");
                pst.setString(3, contactAddress);
                pst.setString(4, "Y");
                pst.executeUpdate();
                
                //Insert organization
                insertStatement = "INSERT INTO WPAGE_METADATA"
                        + "(META_DATA_ID,WPAGE_ID,META_DATA_FIELD,META_DATA_VALUE,RECORD_UPDATED) "
                        + " VALUES (META_DATA_SEQUENCE.NEXTVAL,?,?,?,?)";
                pst = con.prepareStatement(insertStatement);
                pst.setLong(1, pageId);
                pst.setString(2, contactType+"_ORGANIZATION");
                pst.setString(3, contactOrganization);
                pst.setString(4, "Y");
                pst.executeUpdate();
                pst.close();
            }
            
            //Insert raw json
            String insertStatement = "INSERT INTO WPAGE_METADATA"
                    + "(META_DATA_ID,WPAGE_ID,META_DATA_FIELD,META_DATA_VALUE,RECORD_UPDATED) "
                    + " VALUES (META_DATA_SEQUENCE.NEXTVAL,?,?,?,?)";
            pst = con.prepareStatement(insertStatement);
            pst.setLong(1, pageId);
            pst.setString(2, "RAW_JSON");
            pst.setString(3, "REDIRECT TO RAW_METADATA");
            pst.setString(4, "Y");
            pst.executeUpdate();
            pst.close();
            
            insertStatement = "INSERT INTO RAW_METADATA"
                    + "(META_DATA_ID,RAW_DATA) "
                    + " VALUES (META_DATA_SEQUENCE.CURRVAL,?)";
            pst = con.prepareStatement(insertStatement);
            pst.setString(1, jsonString);
            pst.executeUpdate();
            pst.close();
            
        } catch (JSONException | SQLException ex) {
            Logger.getLogger(WebCrawler.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        finally{
            try{
                if(pst!=null)
                    pst.close();
            }catch(Exception ignore){}
        }
        
        return true;
    }
    
    /*
     * Method saves data from builtwith website in the db and file location
     * specified.
     */
    public boolean saveBuiltWith(String url,String destLocation, Connection con, boolean forceUpdate)
    {
        //Access to built with only if db is provided. Done to avoid potential overuse of builtwith API calls.
        if(con == null)
            return false;
        
        //String baseUrl="http://api.builtwith.com/v5/api.xml?KEY="+builtWithKey+"&LOOKUP="+url;
        String baseUrl = "http://api.builtwith.com/v5/api.json?KEY="+builtWithKey+"&LOOKUP="+url;
        Statement st = null;
        ResultSet rs = null;
        PreparedStatement pst = null;
        try{
            st = con.createStatement();
            //If forceUpdate is false, then check if the builtwith details are already available.
            if(!forceUpdate){
                //rs = st.executeQuery("SELECT * FROM WPAGE_BUILTWITH BWITH,WPAGE PAGE WHERE PAGE.WPAGE_ID = BWITH.WPAGE_ID AND PAGE.WPAGE_NAME = 'http://"+url+"'");
                rs = st.executeQuery("SELECT * FROM WPAGE WHERE WPAGE_NAME = 'http://"+url+"'");
                
                if(!rs.next())
                    return false; 

                long WPageID = rs.getLong("WPAGE_ID");
                
                if(rs !=null && !rs.isClosed())
                    rs.close();
                rs = st.executeQuery("SELECT * FROM WPAGE_BUILTWITH WHERE WPAGE_ID = '"+WPageID+"'");
                if(rs.next())
                    return true;
            }
            
            //XmlPage page = webClient.getPage(baseUrl);
            URL myURL = new URL(baseUrl);
            URLConnection myURLConnection = myURL.openConnection();
            myURLConnection.connect();
            BufferedReader in = new BufferedReader(new InputStreamReader(
                                    myURLConnection.getInputStream()));
            String jsonStr = in.readLine();
            
            FileOutputStream fout = new FileOutputStream (destLocation+"builtWith.json");
            BufferedWriter bout = new BufferedWriter(new OutputStreamWriter(fout));
            bout.write(jsonStr);
            bout.flush();
            
            //Update the database if connection is provided
            if(con != null){
                if(rs !=null && !rs.isClosed())
                    rs.close();
                
                if(pst != null && !pst.isClosed())
                    pst.close();
                pst = con.prepareStatement("SELECT * FROM WPAGE WHERE WPAGE_NAME = ?");
                pst.setString(1, "http://"+url);
                
                rs = pst.executeQuery();
                if(!rs.next())
                    return false;
                long WPageID = rs.getLong("WPAGE_ID");
                
                if(rs !=null && !rs.isClosed())
                    rs.close();
                rs = st.executeQuery("SELECT * FROM WPAGE_BUILTWITH WHERE WPAGE_ID = '"+WPageID+"'");
                
                if(!rs.next()){
                    String insertStatement = "INSERT INTO WPAGE_BUILTWITH"+
                        "(WPAGE_ID,BUILTWITH_XML,UPDATE_DATE) VALUES"+ "(?,?,SYSDATE)";
                    pst = con.prepareStatement(insertStatement);
                    pst.setLong(1, WPageID);
                    pst.setString(2, jsonStr);
                    pst.executeUpdate();
                    pst.close();
                }
                else{
                    String insertStatement = "UPDATE WPAGE_BUILTWITH SET BUILTWITH_XML = ?,UPDATE_DATE=SYSDATE WHERE "+
                        "WPAGE_ID = ?";
                    pst = con.prepareStatement(insertStatement);
                    pst.setString(1, jsonStr);
                    pst.setLong(2, WPageID);
                    pst.executeUpdate();
                    pst.close();
                }
                st.close();
                parseStoreBuiltWith(con,WPageID);
            }
        }
        catch(IOException e)
        {
            System.out.println("An error occured "+e);
            Logger.getLogger(WebCrawler.class.getName()).log(Level.SEVERE, null, e);
            return false;
        }
        catch(SQLException e){
            System.out.println("An error occured "+e);
            Logger.getLogger(WebCrawler.class.getName()).log(Level.SEVERE, null, e);
            return false;
        }
        catch(Exception e)
        {
            System.out.println("An error occured "+e);
            Logger.getLogger(WebCrawler.class.getName()).log(Level.SEVERE, null, e);
            return false;
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
     * Given a db connection and a webpage_ID, this method fetches the stored
     * BuiltwithXML and parses it to populate the detailed tables.
     * 
     */
    public boolean parseStoreBuiltWith(Connection dbCon, long WPageID){
        boolean ret = true;
        ResultSet rs = null;
        Statement st = null;
        PreparedStatement pst = null;
        
        try{
            st = dbCon.createStatement();
            rs = st.executeQuery("SELECT * FROM WPAGE_BUILTWITH WHERE WPAGE_ID = "+WPageID);
            if(!rs.next())
                return false;
            
            String jsonString = rs.getString("builtwith_xml");
            JSONObject json = new JSONObject(jsonString);
            
            //If no results found, then either the builtwith data limit exceeded or something went wrong.
            if(json.getJSONArray("Results").length() == 0)
                return false;
            
            JSONArray jsonPaths = json.getJSONArray("Results").getJSONObject(0).getJSONObject("Result").getJSONArray("Paths");
            JSONObject jsonMeta = json.getJSONArray("Results").getJSONObject(0).getJSONObject("Meta");
            
            //Update jsonMeta in WPage_Builtwith table
            String updateStatement = "UPDATE WPAGE_BUILTWITH SET META_DATA=? WHERE WPAGE_ID = ?";
            pst = dbCon.prepareStatement(updateStatement);
            pst.setString(1, jsonMeta.toString());
            pst.setLong(2, WPageID);
            pst.executeUpdate();
            pst.close();
            
            //Loop through all the paths and update the builthWith details tables
            for(int i=0;i<jsonPaths.length();i++){
                JSONObject jsonPath = jsonPaths.getJSONObject(i);
                String url = jsonPath.getString("Url");
                String domain = jsonPath.getString("Domain");
                String subDomain = jsonPath.getString("SubDomain");
                JSONArray technologies = jsonPath.getJSONArray("Technologies");
                
                if(url.isEmpty())
                    url = "none";
                if(domain.isEmpty())
                    domain = "none";
                if(subDomain.isEmpty())
                    subDomain = "none";
                
                long builtWithId;
                if(rs !=null && !rs.isClosed())
                    rs.close();
                pst = dbCon.prepareStatement("SELECT * FROM BUILTWITH_KEY WHERE DOMAIN = ? AND URL = ? AND SUBDOMAIN = ?");
                pst.setString(1, domain);
                pst.setString(2, url);
                pst.setString(3, subDomain);
                rs = pst.executeQuery();
                
                if(rs.next())
                {
                    builtWithId = rs.getLong("BUILTWITH_ID");
                    updateStatement = "UPDATE BUILTWITH_KEY SET DOMAIN=?,URL=?,SUBDOMAIN=?,WPAGE_ID=? where BUILTWITH_ID =?";
                    if(!pst.isClosed())
                        pst.close();
                    pst = dbCon.prepareStatement(updateStatement);
                    pst.setString(1, domain);
                    pst.setString(2, url);
                    pst.setString(3, subDomain);
                    pst.setLong(4, WPageID);
                    pst.setLong(5, builtWithId);
                    pst.executeUpdate();
                    pst.close();
                }else{
                    String insertStatement = "INSERT INTO BUILTWITH_KEY"+
                        "(BUILTWITH_ID,DOMAIN,URL,SUBDOMAIN,WPAGE_ID) VALUES"+ 
                        "(BUILTWITH_KEY_SEQUENCE.NEXTVAL,?,?,?,?)";
                    if(!pst.isClosed())
                        pst.close();
                    pst = dbCon.prepareStatement(insertStatement);
                    pst.setString(1, domain);
                    pst.setString(2, url);
                    pst.setString(3, subDomain);
                    pst.setLong(4, WPageID);
                    pst.executeUpdate();
                    pst.close();
                }
                
                if(rs !=null && !rs.isClosed())
                    rs.close();
                rs = st.executeQuery("SELECT * FROM BUILTWITH_KEY WHERE DOMAIN = '"+domain+"' AND URL = '"+url+"' AND SUBDOMAIN = '"+subDomain+"'");
                if(rs.next())
                    builtWithId = rs.getLong("BUILTWITH_ID");
                else
                    return false;
                        
                for(int j=0;j<technologies.length();j++){
                    String techName = technologies.getJSONObject(j).getString("Name");
                    String tag = technologies.getJSONObject(j).getString("Tag");
                    long firstDetected = technologies.getJSONObject(j).getLong("FirstDetected");
                    long lastDetected = technologies.getJSONObject(j).getLong("LastDetected");
                    
                    if(rs !=null && !rs.isClosed())
                        rs.close();
                    pst = dbCon.prepareStatement("SELECT * FROM BUILTWITH_DETAIL WHERE BUILTWITH_ID = ? AND TECHNOLOGY_NAME = ?");
                    pst.setLong(1, builtWithId);
                    pst.setString(2, techName);
                    rs = pst.executeQuery();
                    
                    if(!rs.next())
                    {
                        String insertStatement = "INSERT INTO BUILTWITH_DETAIL"
                                + "(BUILTWITH_ID,TECHNOLOGY_VALUE,TECHNOLOGY_NAME,CATEGORY,FIRST_DETECTED,LAST_DETECTED)"
                                + "VALUES(?,?,?,?,?,?)";
                        if(!pst.isClosed())
                            pst.close();
                        pst = dbCon.prepareStatement(insertStatement);
                        pst.setLong(1, builtWithId);
                        pst.setString(2, techName);
                        pst.setString(3, tag);
                        pst.setString(4, "");
                        pst.setDate(5, new java.sql.Date(firstDetected));
                        pst.setDate(6, new java.sql.Date(lastDetected));
                        pst.executeUpdate();
                        pst.close();
                    }else{
                        updateStatement = "UPDATE BUILTWITH_DETAIL SET "
                                + "TECHNOLOGY_VALUE=?,CATEGORY=?,FIRST_DETECTED=?,LAST_DETECTED=?"
                                + "WHERE BUILTWITH_ID=? AND TECHNOLOGY_NAME=?";
                        if(!pst.isClosed())
                            pst.close();
                        pst = dbCon.prepareStatement(updateStatement);
                        pst.setLong(5, builtWithId);
                        pst.setString(1, techName);
                        pst.setString(6, tag);
                        pst.setString(2, "");
                        pst.setDate(3, new java.sql.Date(firstDetected));
                        pst.setDate(4, new java.sql.Date(lastDetected));
                        pst.executeUpdate();
                        pst.close();
                    }
                }
            }
            st.close();
        }
        catch(JSONException | SQLException ex){
            Logger.getLogger(WebCrawler.class.getName()).log(Level.SEVERE, null, ex);
            return false;
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
        return ret;
    }
    
    /*
     * This method saves the webpage in the url to the destination location
     * and also saves the content to the subpage table in the database.
     */
    public boolean storeSubPage(String url,String destLocation,BufferedWriter log, Connection con, long parentPageId)throws IOException
    {
        log.flush();
        PreparedStatement pst=null;
        ResultSet rs = null;
        Statement st = null;
        //Save the html content after execution of the code in the page
        try{
            webClient.getOptions().setThrowExceptionOnFailingStatusCode(false);
            HtmlPage page = webClient.getPage(url);
            webClient.getOptions().setThrowExceptionOnFailingStatusCode(true);
            if(page == null)
                throw new PageNotFoundException(url);
            WebResponse response = page.getWebResponse();
            FileOutputStream fout = new FileOutputStream (destLocation+url.replaceAll("[^a-zA-Z]+","")+".html");
            BufferedWriter bout = new BufferedWriter(new OutputStreamWriter(fout));
            bout.write(response.getContentAsString());
            //Saves all contents of the webpage like a browser would
            File destFile = new File(destLocation+url.replaceAll("[^a-zA-Z]+","")+"_complete.html");
            destFile.delete();
            
            //Update database if connection is provided
            if(con != null){
                con.setAutoCommit(true);
                st = con.createStatement();
                rs = st.executeQuery("SELECT * FROM SUBPAGE WHERE WPAGE_URL = '"+url+"' AND PARENT_PAGE_ID ="+parentPageId);
                
                //Insert the new row
                if(!rs.next()){
                    String insertStatement = "INSERT INTO SUBPAGE "+
                        "(SUBPAGE_ID,WPAGE_URl,WPAGE_CONTENT_HTML,WPAGE_CREATE_DATE,WPAGE_LAST_UPDATED,WPAGE_AVAILABLE,RECORD_UPDATED,PARENT_PAGE_ID) VALUES"+
                            "(SUBPAGE_SEQUENCE.NEXTVAL,?,?,SYSDATE,SYSDATE,?,?,?)";
                    pst = con.prepareStatement(insertStatement);
                    pst.setString(1, url);
                    pst.setString(2, response.getContentAsString());
                    pst.setString(3, "Y");
                    pst.setInt(4, 1);
                    pst.setLong(5, parentPageId);
                    pst.executeUpdate();
                    pst.close();
                    rs.close();
                }
                //Update the new row
                else{
                    long WPageId = rs.getLong("SUBPAGE_ID");
                    String insertStatement = "UPDATE SUBPAGE "+
                        "SET WPAGE_CONTENT_HTML=?, WPAGE_LAST_UPDATED = SYSDATE, RECORD_UPDATED = ? WHERE "+
                            "SUBPAGE_ID = ?";
                    pst = con.prepareStatement(insertStatement);
                    pst.setString(1, response.getContentAsString());
                    pst.setString(2, "Y");
                    pst.setLong(3, WPageId);
                    pst.executeUpdate();
                    pst.close();
                    rs.close();
                }
                st.close();
            }
            page.save(destFile);
        }
        catch(UnknownHostException | PageNotFoundException | MalformedURLException | ClassCastException e)
        {
            log.write("\nHost could not be accessed "+url);
            
            //Update in DB if the host is unresolvable
            try{;
                String insertStatement = "UPDATE SUBPAGE SET WPAGE_LAST_UPDATED=SYSDATE ,WPAGE_AVAILABLE ='N', RECORD_UPDATED = 'Y'"
                        + "WHERE WPAGE_URL = ? and PARENT_PAGE_ID = ?";
                pst = con.prepareStatement(insertStatement);
                pst.setString(1,url);
                pst.setLong(2,parentPageId);
                pst.executeUpdate();
                pst.close();
            }
            catch(SQLException ex)
            {
                Logger.getLogger(WebCrawler.class.getName()).log(Level.SEVERE, null, ex);
            }
            return false;
        }
        catch(Exception e)
        {
            log.write("\nAn error occured while Chrome tried to access "+url+" "+e);
            log.write(e.getStackTrace().toString());
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
     * This method inserts rows in the urlList as entries in the subpage table
     * in the db connection con.
     */
    protected boolean insertSubPage(Set<String> urlList, Connection con, long parentPageId, int depth)throws IOException
    {
        PreparedStatement pst=null;
        ResultSet rs = null;
        Statement st = null;
        //Save the html content after execution of the code in the page
        try{
            //Update database if connection is provided
            if(con != null){
                con.setAutoCommit(true);
                st = con.createStatement();
                //Iterate through the list of urls and insert them in the sub table
                for(String url : urlList){
                    if(url == null || url.isEmpty())
                        continue;
                    pst = con.prepareStatement("SELECT * FROM SUBPAGE WHERE WPAGE_URL =? AND PARENT_PAGE_ID = ?");
                    pst.setString(1, url);
                    pst.setLong(2, parentPageId);
                    rs = pst.executeQuery();
                
                    //Insert the new row
                    if(!rs.next()){
                        String insertStatement = "INSERT INTO SUBPAGE "+
                            "(SUBPAGE_ID,WPAGE_URl,WPAGE_CREATE_DATE,WPAGE_LAST_UPDATED,WPAGE_AVAILABLE,RECORD_UPDATED,PARENT_PAGE_ID,CRAWL_DEPTH) VALUES"+
                                "(SUBPAGE_SEQUENCE.NEXTVAL,?,SYSDATE,SYSDATE,?,?,?,?)";
                        pst.close();
                        pst = con.prepareStatement(insertStatement);
                        pst.setString(1, url);
                        pst.setString(2, "Y");
                        pst.setString(3, "N");
                        pst.setLong(4, parentPageId);
                        pst.setInt(5,depth);
                        pst.executeUpdate();
                        pst.close();
                        rs.close();
                    }
                    //Update the row to refresh the content separately
                    else{
                        long WPageId = rs.getLong("SUBPAGE_ID");
                        String insertStatement = "UPDATE SUBPAGE "+
                            "SET WPAGE_LAST_UPDATED = SYSDATE, RECORD_UPDATED = ? WHERE "+
                                "SUBPAGE_ID = ?";
                        pst.close();
                        pst = con.prepareStatement(insertStatement);
                        pst.setString(1, rs.getString("RECORD_UPDATED"));
                        pst.setLong(2, WPageId);
                        pst.executeUpdate();
                        pst.close();
                        rs.close();
                    }
                }
                st.close();
            }
        }
        catch(SQLException ex)
        {
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
     * This method is used to crawl based on the WPAGE data in the database connection
     * specified to the depth value passed. Crawling begins in the WPage rows
     * and then moves on to the SubPage rows based on the depth.
     * 
     * The crawler pics up only those webpages that are updated (RECORD_UPDATED = 1) from WPAGE table
     * All new pages found are added to subpage table.
     */
    public void crawlWeb(Connection dbCon,int depth)
    {
        ResultSet rs = null;
        Statement st = null;
        try{
            st = dbCon.createStatement();
            rs = st.executeQuery("SELECT * FROM WPAGE WHERE RECORD_UPDATED = 1");
            
            while(rs.next()){
                Set<String> subPageList = new LinkedHashSet();
                StringWebResponse response = new StringWebResponse(rs.getString("WPAGE_CONTENT_HTML"), new URL(rs.getString("WPAGE_NAME")));
                HtmlPage page = null;
                
                //Turn off throwing exception on 404 status code as the page already exists in the db
                webClient.getOptions().setThrowExceptionOnFailingStatusCode(false);
                page = HTMLParser.parseHtml(response, webClient.getCurrentWindow());
                webClient.getOptions().setThrowExceptionOnFailingStatusCode(true);
                
                List hrefs = page.getDocumentElement().getHtmlElementsByTagName(HtmlAnchor.TAG_NAME);
                for(Object obj : hrefs){
                    DomNode dn = (DomNode) obj;
                    Node n = dn.getAttributes().getNamedItem("href");
                    if(n == null)
                        continue;
                    String subPage = n.getNodeValue();
                    subPage = subPage.trim();
                    if(subPage.startsWith("/")){
                        String pageName = rs.getString("WPAGE_NAME");
                        pageName = pageName.substring(pageName.indexOf("://")+3);
                        pageName = "http://www."+pageName;
                        subPage = pageName+subPage;
                    }
                    subPageList.add(subPage);
                }
                insertSubPage(subPageList,dbCon,rs.getLong("WPAGE_ID"),1);
                updateWPageStatus(rs.getLong("WPAGE_ID"),"0",dbCon);
            }
            
            if(rs !=null && !rs.isClosed())
                rs.close();
            rs = st.executeQuery("SELECT * FROM SUBPAGE WHERE RECORD_UPDATED != 'N' AND CRAWL_DEPTH <"+depth);
            
            while(rs.next()){
                Set<String> subPageList = new LinkedHashSet();
                StringWebResponse response = new StringWebResponse(rs.getString("WPAGE_CONTENT_HTML"), new URL(rs.getString("WPAGE_URL")));
                HtmlPage page = HTMLParser.parseHtml(response, webClient.getCurrentWindow());
                List hrefs = page.getDocumentElement().getHtmlElementsByTagName(HtmlAnchor.TAG_NAME);
                for(Object obj : hrefs){
                    DomNode dn = (DomNode) obj;
                    Node n = dn.getAttributes().getNamedItem("href");
                    if(n == null)
                        continue;
                    String subPage = n.getNodeValue();
                    subPage = subPage.trim();
                    if(subPage.startsWith("/")){
                        String pageName = rs.getString("WPAGE_NAME");
                        pageName = pageName.substring(pageName.indexOf("://")+3);
                        pageName = "http://www."+pageName;
                        subPage = pageName+subPage;
                    }
                    subPageList.add(subPage);
                }
                insertSubPage(subPageList,dbCon,rs.getLong("PARENT_PAGE_ID"),rs.getInt("CRAWL_DEPTH")+1);
            }
        }
        catch(SQLException | IOException ex){
            Logger.getLogger(WebCrawler.class.getName()).log(Level.SEVERE, null, ex);
        }
        finally{
            try{
                if(st!=null)
                    st.close();
                if(rs!=null)
                    rs.close();
            }catch(Exception ignore){}
        }
    }
    
    /*
     * Set Record updated status to the string mentioned for the webpage with pageId specified.
     */
    private void updateWPageStatus(long pageId,String recordUpdated,Connection dbCon){
        PreparedStatement pst = null;
        try{
            String updateStatement ="UPDATE WPAGE SET RECORD_UPDATED = ? WHERE WPAGE_ID = ?";
            pst = dbCon.prepareStatement(updateStatement);
            pst.setLong(2, pageId);
            pst.setString(1, recordUpdated);
            pst.executeUpdate();
        }
        catch(SQLException ex){
            Logger.getLogger(WebCrawler.class.getName()).log(Level.SEVERE, null, ex);
        }
        finally{
            try {
                if(pst!=null)
                    pst.close();
            } catch (SQLException ignore) {}
        }
    }
}

class PageNotFoundException extends Exception {
    
    public PageNotFoundException(String url) {
        super(url);
    }
}