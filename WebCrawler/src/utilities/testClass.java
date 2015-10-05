/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package utilities;

import uiSupport.WPageUISupportProcessor;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;
import patternMatcher.IdentificationEngine;
import webcrawler.*;

/**
 *
 * @author Subu
 */
public class testClass {
    public static void main(String[] args) throws SQLException
    {
        //testDB();
        //testParseBuiltWith();
        /*Object obj = null;
        if( obj==null ||obj.equals(obj))
            System.out.println("Success");*/
        //testWebCrawl();
        //testSaveBuiltWith();
        //testWhoIs();
        //testExtractTagsFromHTML();
        //teststoreWpageTags();
        //teststoreWpageText();
        //teststoreFNamesFromPath();
        //testupdateMissingBuilthWith();
        //testupdateAllJDist();
        //teststorePatternMatches();
        //teststoreWpageText2();
        //testupdateAllJDistVocab();
        //testAddingNewPage();
        //testgetHighFreqVocab();
        //testgetSearchResults();
        testgetTopSuggestedLinks();
    }
    
    static void testDB() throws SQLException{
        Connection con = DBConnect.getConnection();
        if(con != null)
        {
            Statement st = con.createStatement();
            ResultSet rs = st.executeQuery("select * from dual");
            rs.next();
            System.out.println(rs.getObject(1));
        }
    }
    
    static void testParseBuiltWith(){
        WebCrawler wc = new WebCrawler();
        wc.parseStoreBuiltWith(DBConnect.getConnection(), 3);
    }
    
    static void testWebCrawl(){
        WebCrawler wc = new WebCrawler();
        wc.crawlWeb(DBConnect.getConnection(),2);
        wc.closeWebCrawler();
    }
    
    static void testWhoIs(){
        WebCrawler wc = new WebCrawler();
        wc.saveWhoIs("http://builtwith.com", DBConnect.getConnection(),3);
        wc.saveWhoIs("http://builtwith.com", DBConnect.getConnection(),3);
        wc.closeWebCrawler();
    }
    
    static void testSaveBuiltWith(){
        WebCrawler wc = new WebCrawler();
        String destLocation = "C:\\Users\\Subu\\Desktop\\Master thesis\\output\\";
        String name = "builtwith.com";
        wc.saveBuiltWith(name, destLocation+"\\"+name+"\\",DBConnect.getConnection(),false);
        wc.closeWebCrawler();
    }
    
    static void testExtractTagsFromHTML()
    {
        DataExtractor dp = new DataExtractor();
        String test = "<html><head>this is head </head><body>this is body</body></html>";
        //dp.extractTagsFromHTML(test);
    }
    
    static void teststoreWpageTags()
    {
        DataExtractor dp = new DataExtractor();
        dp.storeWpageTags(DBConnect.getConnection());
    }
    
    static void teststoreWpageText()
    {
        DataExtractor dp = new DataExtractor();
        dp.storeWpageText(DBConnect.getConnection(),false);
    }
    
    static void teststoreWpageText2()
    {
        DataExtractor dp = new DataExtractor();
        dp.storeWpageText(DBConnect.getConnection(),true);
    }
    
    static void teststoreFNamesFromPath()
    {
        DataExtractor dp = new DataExtractor();
        dp.storeFNamesFromPath("C:\\Users\\Subu\\Desktop\\Master thesis\\output\\",DBConnect.getConnection());
    }
    
    static void testupdateMissingBuilthWith()
    {
        DataExtractor dp = new DataExtractor();
        try {
            dp.updateMissingBuilthWith("C:\\Users\\Subu\\Desktop\\Master thesis\\output\\",DBConnect.getConnection());
        } catch (InterruptedException ex) {
            Logger.getLogger(testClass.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    static void testupdateAllJDist()
    {
        DataProcessor dp;
        try {
            dp = new DataProcessor(DBConnect.getConnection());
            dp.updateAllJDist(DBConnect.getConnection(), 11111);
        } catch (SQLException ex) {
            Logger.getLogger(testClass.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    static void teststorePatternMatches(){
        IdentificationEngine ie = new IdentificationEngine();
        ie.storePatternMatches(DBConnect.getConnection());
    }
    
    static void testupdateAllJDistVocab(){
        DataProcessor dp;
        try {
            dp = new DataProcessor(DBConnect.getConnection());
            dp.updateAllJDist(DBConnect.getConnection(), 10000);
        } catch (SQLException ex) {
            Logger.getLogger(testClass.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    static void testAddingNewPage(){
        WPageUISupportProcessor uip = new WPageUISupportProcessor(DBConnect.getConnection(),"google.com","C:\\Users\\Subu\\Desktop\\Master thesis\\output\\");
    }
    
    static void testgetHighFreqVocab(){
        try {
            VocabMiner vMine = new VocabMiner(DBConnect.getConnection());
            vMine.getHighFreqVocab();
        } catch (SQLException ex) {
            Logger.getLogger(testClass.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    static void testgetSearchResults(){
        try {
            VocabMiner vMine = new VocabMiner(DBConnect.getConnection());
            vMine.getSearchResults("Paris Hilton");
        } catch (SQLException ex) {
            Logger.getLogger(testClass.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    static void testgetTopSuggestedLinks(){
        try {
            VocabMiner vMine = new VocabMiner(DBConnect.getConnection());
            vMine.getTopSuggestedLinks(10);
        } catch (SQLException ex) {
            Logger.getLogger(testClass.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
