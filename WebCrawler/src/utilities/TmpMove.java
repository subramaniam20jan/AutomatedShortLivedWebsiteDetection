/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package utilities;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 *
 * @author Subu
 */
public class TmpMove {
    public static void main(String args[]) throws IOException{
        try{
            String url = "fx-supplements.com";
            String str = readFile("C:\\Users\\Subu\\Desktop\\Master thesis\\sample output\\Largest\\"+url+"\\datacomplete.html");
            Connection con = DBConnect.getConnection();
            PreparedStatement pst = con.prepareStatement("UPDATE WPAGE SET WPAGE_CONTENT_HTML =? WHERE WPAGE_NAME=?");
            pst.setString(1, str);
            pst.setString(2, "http://"+url);
            pst.executeUpdate();
        }
        catch(SQLException ex){
            System.out.println(ex);
        }
    }
    
    static String readFile(String path) 
    throws IOException 
    {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new String(encoded, Charset.defaultCharset());
    }
}
