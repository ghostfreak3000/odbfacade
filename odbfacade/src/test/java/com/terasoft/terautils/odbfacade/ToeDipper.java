package com.terasoft.terautils.odbfacade;

import java.util.List;

import com.terasoft.terautils.odbfacade.ODBFacade.IO_TYP;

public class ToeDipper {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ODBFacade db = new ODBFacade("plocal:F:\\Mcrops\\tmp\\db", "admin", "admin");
		db.printToFile("Image", "C:\\Users\\Bishaka\\Desktop\\test.txt");
	}
	
	public static void main1(){
		ODBFacade db = new ODBFacade("plocal:F:/ToolProjects/2015/OrientDB/tmp/db_odbfacade","admin","admin");
		
		@SuppressWarnings("unchecked")
		List<Apple> apples = (List<Apple>)db.read("Apple", IO_TYP.READ_STORAGE_LIST_OBJS, Apple.class);
		
		for(Apple apple : apples)
		{
			System.out.println("Apple< "+apple.getColor()+","+apple.getAge()+" >");
		}		
	}

}
