/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package webcrawler;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

/**
 *
 * @author Subu
 * 
 * The class defines functionality that is used to mine the most recurring key words in 
 * the webpage database used.
 * 
 * It is also used to retrieve the Google search results for queries.
 */
public class VocabMiner {
    
    private Connection dbCon = null;
    private static List<String> commonExclStrings = new ArrayList();
    private static List<String> mineExclStrings = new ArrayList();
    
    /*
     * The constructor takes the db connection argument that is used for the entire 
     * set of methods defined in the class.
     * 
     * It also builds the blacklisted words list from the db based on the connection.
     */
    public VocabMiner(Connection con) throws SQLException{
        dbCon = con;
        
        ResultSet rs = null;
        Statement st = null;
        try{
            st = dbCon.createStatement();
            rs = st.executeQuery("SELECT * FROM BLACKLIST WHERE BLACKLIST_TYPE='JDIST_VOCAB'");
            while(rs.next()){
                commonExclStrings.add(rs.getString("BLACKLIST_VALUE"));
            }
            rs.close();
            
            rs = st.executeQuery("SELECT * FROM BLACKLIST WHERE BLACKLIST_TYPE='VOCAB_FREQ'");
            while(rs.next()){
                mineExclStrings.add(rs.getString("BLACKLIST_VALUE"));
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
     * Queries the DB for all the phrases used, filters the blacklisted words
     * and returns a list of all the unique strings and their corresponding
     * number of occurences in the entire db.
     */
    public Map<String,Integer> getHighFreqVocab(){
        Map<String,Integer> wordMap = new HashMap<>();
        ResultSet rs = null;
        Statement st = null;
        
        try{
            st = dbCon.createStatement();
            rs = st.executeQuery("SELECT meta_data_value FROM wpage,wpage_metadata WHERE "
                    + "meta_data_field LIKE 'HTML_TEXT%' AND wpage.wpage_upvote = '1'"
                    + "AND wpage.wpage_id = wpage_metadata.wpage_id");
            
            while(rs.next()){
                String source = rs.getString("META_DATA_VALUE");
                wordMap = addAllWordCounts(source,wordMap);
            }
            
            //Prints all the words in the word list with the frequency of use
            /*Set<String> words = wordMap.keySet();
            for(String eachWord:words){
                System.out.println(eachWord+","+wordMap.get(eachWord));
            }*/
        }
        catch(SQLException ex){
            Logger.getLogger(VocabMiner.class.getName()).log(Level.SEVERE, null, ex);
        }
        finally{
            try{
                if(rs!=null && !rs.isClosed())
                    rs.close();
                if(st!=null && !rs.isClosed())
                    st.close();
            }catch(Exception ignore){}
        }
        
        return wordMap;
    }
    
    /*
     * Given a map and a source string containing a text passage, breaks the 
     * text into words and returns the map updated with the word to word count (ie. the number of
     * times the word occured in the passage) mapping.
     */
    private static Map addAllWordCounts(String str,Map<String,Integer> map){
        
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
        
        for(String eachStr:str.split(" ")){
            eachStr = eachStr.trim().toLowerCase();
            if(!commonExclStrings.contains(eachStr) && !isNumeric(eachStr) && eachStr.length()>1 && !eachStr.contains("$") && !mineExclStrings.contains(eachStr))
                map = addToHashMap(map,eachStr.trim());
        }
        
        return map;
    }
    
    /*
     * Method takes a map and adds the string and the number of occurences for the
     * string in the map as well.
     */
    private static Map addToHashMap(Map<String,Integer> map,String str){
        
        if(map.containsKey(str))
            map.put(str, map.get(str)+1);
        else
            map.put(str, 1);
        
        return map;
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
    
    /*
     * Method takes the search query and returns the title and url results of the top 10
     * search results from Google.
     */
    public Map<String,String> getSearchResults(String query){
        Map<String,String> ret = new HashMap();
        
        try {
            String google = "http://www.google.com/search?q=";
            String charset = "UTF-8";
            String userAgent = "Subramaniam"; // Change this to your company's name and bot homepage!

            Elements links = Jsoup.connect(google + URLEncoder.encode(query, charset)).userAgent(userAgent).get().select("li.g>h3>a");

            for (Element link : links) {
                String title = link.text();
                String url = link.absUrl("href"); // Google returns URLs in format "http://www.google.com/url?q=<url>&sa=U&ei=<someKey>".
                url = URLDecoder.decode(url.substring(url.indexOf('=') + 1, url.indexOf('&')), "UTF-8");

                if (!url.startsWith("http")) {
                    continue; // Ads/news/etc.
                }
                
                
                //Prints the webpages returned by google
                //System.out.println("Title: " + title);
                //System.out.println("URL: " + url);
                ret.put(url, title);
            }
            
        } catch (MalformedURLException ex) {
            Logger.getLogger(VocabMiner.class.getName()).log(Level.SEVERE, null, ex);
        }
        catch(IOException ex){
            Logger.getLogger(VocabMiner.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return ret;
    }
    
    /*
     * This method returns the top search result links for the top key words from the 
     * functions getSearchResults() and getHighFreqVocab() in this class
     * 
     * 'topResult' number of hishest frequency key words from the database are used
     * to obtain the search results.
     */
    public Map<String,String> getTopSuggestedLinks(int topResult){
        Map<String,String> ret = new HashMap<>();
        
        Map<String,Integer> vocab = getHighFreqVocab();
        vocab = sortByValue(vocab);
        
        Object[] words = vocab.keySet().toArray();
        if(topResult>words.length)
            topResult = words.length;
        
        for(int i=0;i<topResult;i++){
            String eachWord = (String) words[i];
            ret.putAll(getSearchResults(eachWord+" buy online"));
        }
        
        return ret;
    }
    
    /*
     * Given a map, it returns a map sorted by the values
     */
    private static Map sortByValue(Map map) {
        List list = new LinkedList(map.entrySet());
        Collections.sort(list, new Comparator() {
            public int compare(Object o1, Object o2) {
                return ((Comparable) ((Map.Entry) (o2)).getValue())
                .compareTo(((Map.Entry) (o1)).getValue());
            }
        });

        Map result = new LinkedHashMap();
        for (Iterator it = list.iterator(); it.hasNext();) {
            Map.Entry entry = (Map.Entry)it.next();
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }
}