/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package uiSupport;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import webcrawler.VocabMiner;

/**
 *
 * @author Subu
 */
public class VocabMinerUISupportProcessor {
    
    Connection dbCon;
    VocabMiner miner;
    
    public VocabMinerUISupportProcessor(Connection con) throws SQLException{
        dbCon = con;
        miner = new VocabMiner(con);
    }
    
    /*
     * Method returns a list of the top results for num number of keywords.
     * 
     * Returns a map of the link and the title
     */
    public Map<String,String> getTopResults(int num){
        return miner.getTopSuggestedLinks(num);
    }
    
    /*
     * Method returns the top num keywords from the database
     */
    public List<String> getTopKeyWords(int num){
        List<String> ret = new ArrayList<>();
        
        Set keywords = sortByValue(miner.getHighFreqVocab()).keySet();
        for(int i=0;i<num;i++)
            ret.add((String)keywords.toArray()[i]);
        
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
