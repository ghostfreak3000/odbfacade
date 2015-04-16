package com.terasoft.terautils.odbfacade;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.terasoft.terautils.odbfacade.ODBFacade.SRCH_TYP;

public class ODBFacadeSearchTest {

	private static ODBFacade db;
	
	private static String 		file_path = ODBFacadeSearchTest.class.getResource("/odb.odbmap.xml").getFile(), 
								db_path="plocal:F:/ToolProjects/2015/OrientDB/tmp/db_odbfacade", 
								user = "admin", 
								pass = "admin";

	private String doc;
	
	@BeforeClass
	public static void bootStrap(){		
		db = new ODBFacade(file_path);
		db.create();
	}
		
	@Before
	public void setup(){
		db = new ODBFacade(file_path);
		doc = "Apple";
	}
	
	@Test
	public void canSearch() throws Exception{
		
		int size_before = db.search(doc,"color","Orange").size();		
		String[] apples = new String[]{ db.add(new Apple("Orange", "5"), doc),db.add(new Apple("Orange", "5"), doc) };		
		
		int size_After = db.search(doc,"color","Orange").size();		
		assertEquals("Storage for <"+doc+"> filtered by <Color = Orange> should be bigger by <"+apples.length+"> from <"+size_before+"> ",size_before+apples.length, size_After);		
	}

	@Test
	public void canSearch_returnsObjList() throws Exception{
		
		String t2 = Apple.class.getName();
		String t1 = ((List<?>) db.search(doc,"color","Orange",Apple.class)).get(0).getClass().getName();				
		assertEquals("Types should be the same",t2, t1);		
	}

	@Test
	public void canSearch_nonExistingClassReturnsZero() throws Exception{
		assertEquals(	"Non existant class shld return sero", 
						0, db.search("FakeStore", "FakeField", "FakeValue").size());
	}
	
	@Test
	public void canSearch_withMultipleFilters() throws Exception{
	
		String 	color = "Magenta",
				age = "25";
		
		Apple a1 = new Apple(color, age);
		Map<String, String> props = new HashMap<String, String>();
		
		Map<ODBFacade.SRCH_TYP, Map<String, String> > filters = new HashMap<ODBFacade.SRCH_TYP, Map<String,String>>();
		
		props.put("color", color);
		props.put("age", age);
		
		filters.put(SRCH_TYP.LK, props);
		
		int size_before = ((List<ODocument>) db.search(doc,filters)).size();
		db.add(a1, doc);
		
		int size_after = ((List<ODocument>) db.search(doc,filters)).size();		
		assertEquals("Storage for <"+doc+"> filtered by <Color = "+color+">,<age = "+age+"> should be bigger by <"+1+"> from <"+size_before+"> ",size_before+1, size_after);
	}
	
	@Test
	public void canSearch_SrchTyp_eq() throws Exception{
		
		String filter_1 = "Viol",
			   filter_2 = "violate",
			   filter_3 = "Violate";

		Apple apple = new Apple(filter_3, "4");
		
		int size_1 = db.search(doc, "color", filter_1).size();
		int size_2 = db.search(doc, "color", filter_2, SRCH_TYP.EQ).size();
		int size_3 = db.search(doc, "color", filter_3, SRCH_TYP.EQ).size();
		
		db.add(apple, doc);
		
		canSearch_SrchTyp_eq_assert("LK", "color", filter_1, "1", size_1, size_1+1, db.search(doc, "color", filter_1).size());
		canSearch_SrchTyp_eq_assert("EQ", "color", filter_2, "0", size_2, size_2, db.search(doc, "color", filter_2,SRCH_TYP.EQ).size());
		canSearch_SrchTyp_eq_assert("EQ", "color", filter_3, "1", size_3, size_3+1, db.search(doc, "color", filter_3,SRCH_TYP.EQ).size());
	}
	
	private void canSearch_SrchTyp_eq_assert(String filter,String field, String value, String larger, int previous, int expected, int actual )
	{
		assertEquals("Results for Filter<"+filter+"> on field<"+field+"> for value<"+value+"> should be larger by<"+larger+"> from <"+previous+">", expected, actual);
	}
	
}
