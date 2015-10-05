/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package webcrawler;

import java.io.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import utilities.DBConnect;

/**
 *
 * @author Subu
 */
public class WebCrawlerStarter {
    
    public static void main(String[] args)throws IOException
    {
        String destLocation = "";
        String inputLocation="";
        if(args.length <2)
        {
            System.out.println("Invalid arguments");
            System.exit(0);
        }
        else
        {
            destLocation = args[0];
            inputLocation = args[1];
        }
        
        
        //destLocation = "C:\\Users\\Subu\\Desktop\\Master thesis\\output\\";
        //inputLocation="C:\\Users\\Subu\\Desktop\\Master thesis\\input\\input.txt";
        FileOutputStream fLog = new FileOutputStream(destLocation+"log.txt");
        BufferedWriter log = new BufferedWriter(new OutputStreamWriter(fLog));
        
        //Read input websites from the file
        FileInputStream fin = new FileInputStream(inputLocation);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fin));
        
        //Call method to store all webpages from the input file
        WebCrawler wc = null;
        try{
            wc = new WebCrawler();
            Connection dbCon =DBConnect.getConnection();
            
            //Store webpages from fle
            storeWebPagesFromFile(reader,log,destLocation,wc,dbCon);
            
            //Crawl the webpage stored in the db
            crawlStarter(log,destLocation,wc,dbCon);
        }
        catch(IOException | InterruptedException ex){
            Logger.getLogger(WebCrawlerStarter.class.getName()).log(Level.SEVERE, null, ex);
        }
        finally{
            wc.closeWebCrawler();
            System.exit(0);
        }
    }
    
    /*
     * Method used to crawl through all webpages in the db specifed by dbCon and carve out the links.
     * 
     * It then stores the links to the subPages table and downloads all the contents to the appropriate 
     * tables as well.
     */
    public static void crawlStarter(BufferedWriter log, String destLocation,WebCrawler wc, Connection dbCon) throws IOException, InterruptedException
    {
        log.write("Starting to crawl the web "+new Date().toString());
        Thread.sleep(1);
        Statement st = null;
        ResultSet rs = null;
        try{
            //Crawl the data in the db to desired depth - depth 1
            wc.crawlWeb(dbCon, 1);
            
            //Update the subpage tables with data once crawling is complete
            st = dbCon.createStatement();
            rs = st.executeQuery("select * from subpage where record_updated = 'N'");
            
            while(rs.next()){
                File dir = new File(destLocation+rs.getString("WPAGE_URL").replaceAll("[^a-zA-Z]+","")+"\\");
                if(!dir.exists())
                    dir.mkdir();
                wc.storeSubPage(rs.getString("WPAGE_URL"), destLocation+"\\subpages\\"+rs.getString("WPAGE_URL").replaceAll("[^a-zA-Z]+","")+"\\", log, dbCon, rs.getLong("PARENT_PAGE_ID"));
            }
        }
        catch(SQLException | IOException ex){
            Logger.getLogger(WebCrawlerStarter.class.getName()).log(Level.SEVERE, null, ex);
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
     * Method used to store the webpages from the input file specified in reader to
     * the database and retrieve the builtwith information if available.
     * 
     * Also saves content of the actual webpage(including images) to the destoLocation specifed.
     */
    public static void storeWebPagesFromFile(BufferedReader reader,BufferedWriter log,String destLocation,WebCrawler wc,Connection dbCon) throws IOException, InterruptedException{
        String url;
        String name = reader.readLine();
        
        log.write("Begin run on "+ new Date().toString());
        
        //Retrieve the urls from the file and store them
        while(name != null){
            name = name.trim();
            if(!(Character.isLetter(name.charAt(0))||Character.isDigit(name.charAt(0)))){
                name = name.substring(1);
            }
            if(name.isEmpty()){
                name = reader.readLine();
                continue;
            }
            new File(destLocation+name).mkdir();
            url = "http://"+name;
            Thread.sleep(1000);
            
            //Store the main webpage
            if(!wc.storeWebPage(url,destLocation+"\\"+name+"\\",log,dbCon,false)){
                log.write("\nProblem Accessing website "+url+" "+new Date().toString());
            }
            else{
                log.write("\nSuccessfully saved "+url+" "+new Date().toString());
                //Store builtwith data
                wc.saveBuiltWith(name, destLocation+"\\"+name+"\\",dbCon,false);
            }
            name = reader.readLine();
        }
        
        log.write("Ending run on "+ new Date().toString());
        log.flush();
    }
}