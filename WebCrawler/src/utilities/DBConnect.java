/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package utilities;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 *
 * @author Subu
 */
public class DBConnect {
    static Connection con = null;
    static String username = "web_crawler";
    static String password = "password1";
    
    public static Connection getConnection(){
        if(con != null)
            return con;
        
        try{
            con = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521/xe",username,password);
        }
        catch(SQLException e)
        {
            e.printStackTrace();
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        return con;
        
    }
}
