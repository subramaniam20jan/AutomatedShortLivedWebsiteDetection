/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;

import javax.swing.JFrame;
import javax.swing.JTextField;

import org.cef.CefApp;
import org.cef.CefClient;
import org.cef.OS;
import org.cef.browser.CefBrowser;

/**
 *
 * @author Subu
 */
public class BrowserFrame extends javax.swing.JFrame{
    
   long serialVersionUID = -5570653778104813836L;
   JTextField address_;
   CefApp cefApp_;
   CefClient  client_;
   CefBrowser browser_;
   Component  browerUI_;
  
   BrowserFrame(String startURL, boolean useOSR, boolean isTransparent) {
        CefApp cefApp_ = CefApp.getInstance();
        client_ = cefApp_.createClient();
        browser_ = client_.createBrowser(startURL, useOSR, isTransparent);
        browerUI_ = browser_.getUIComponent();
        address_ = new JTextField(startURL, 100);
        address_.addActionListener(new ActionListener() {
        @Override
        public void actionPerformed(ActionEvent e) {
            browser_.loadURL(address_.getText());
        }
        });
        getContentPane().add(address_, BorderLayout.NORTH);
        getContentPane().add(browerUI_, BorderLayout.CENTER);
        pack();
        setSize(800,600);
        setVisible(true);
    }
     
}
