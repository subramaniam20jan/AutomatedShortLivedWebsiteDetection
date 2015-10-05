/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package utilities;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import webcrawler.DataExtractor;

/**
 *
 * @author Subu
 */
public class TmpTest {
    
    public static void main(String args[])
    {
        DataExtractor de = new DataExtractor();
        Statement st;
        ResultSet rs;
        try{
            st = DBConnect.getConnection().createStatement();
            rs = st.executeQuery("SELECT * FROM WPAGE WHERE WPAGE_ID = 24 ");
            if(rs.next()){
                //de.extractTextFromHTML(rs.getString("WPAGE_CONTENT_HTML"));
            }
        }
        catch(SQLException ex){
            System.out.println(ex);
        }
    }
    
}
