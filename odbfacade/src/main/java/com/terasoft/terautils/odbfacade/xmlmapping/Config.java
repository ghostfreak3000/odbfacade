/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.terasoft.terautils.odbfacade.xmlmapping;

import java.util.ArrayList;
import java.util.List;
/**
 *
 * @author Bishaka
 */
public class Config {
    
    private String path;
    
    private String user;
    
    private String pass;
    
    List<Cls> cls = new ArrayList<Cls>();
    
    List<Lnk> lnk = new ArrayList<Lnk>();

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public List<Cls> getCls() {
        return cls;
    }

    public void setCls(List<Cls> cls) {
        this.cls = cls;
    }

    public List<Lnk> getLnk() {
        return lnk;
    }

    public void setLnk(List<Lnk> lnk) {
        this.lnk = lnk;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPass() {
        return pass;
    }

    public void setPass(String pass) {
        this.pass = pass;
    }
    
}
