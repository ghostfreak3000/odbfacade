package com.terasoft.terautils.odbfacade;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.terasoft.terautils.odbfacade.ODBFacade.IO_TYP;
import com.terasoft.terautils.odbfacade.exceptions.NoDBFoundException;
import com.terasoft.terautils.odbfacade.exceptions.Unsupported_IO_TYP_Exception;
import com.terasoft.terautils.odbfacade.exceptions.WrongCredException;

public class ODBFacadeTest {

	/*TO TEST
	 * 
	 * canRead_returnListObjs
	 * 
	 * canRead_returnListObjs_JSON
	 * */
	
	private static ODBFacade db;
	
	private static String 		file_path = ODBFacadeTest.class.getResource("/odb.odbmap.xml").getFile(), 
								db_path="plocal:F:/ToolProjects/2015/OrientDB/tmp/db_odbfacade", 
								user = "admin", 
								pass = "admin";

	private static ObjectMapper JSON = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);;
	
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
	public void canLoadParametersFromXmlFile() throws Exception
	{				
		assertEquals("Database path should be : " + db_path,	db_path, 	db.getPath());
		assertEquals("Database user should be : " + user, 	user, 	db.getUser());
		assertEquals("Database pass should be : " + pass,	pass, 	db.getPass());		
	}	
	
	@Test
	public void canConnectToDatabase() throws Exception{		
		assertTrue("Facade shld be able to connect to DB",db.canConn());
	}

	@Test
	public void canConnectToDatabase_throwsExceptionWithWrongUnameOrPass() throws Exception{		
		assertTrue("Database shld Exist by now",db.canConn());
		
		db.setUser("bishaka");
		try{
			db.canConn();
			fail("Username<"+db.getUser()+"> shldn't work for db on path <"+db.getPath()+">");
		}catch(WrongCredException ndbfe)
		{
			assertEquals("Wong username or password for db found on path <"+db.getPath()+">", ndbfe.getMessage());
		}
	}
		
	@Test
	public void canConnectToDatabase_throwsExceptionWhenNoDbFound() throws Exception{
		db.setPath("plocal:tests/com/terasoft/odb/db_fake");
		try{
			db.canConn();
			fail("There shldn't be any db on path <"+db.getPath()+">");
		}catch(NoDBFoundException ndbfe)
		{
			assertEquals("No db found on path <"+db.getPath()+">", ndbfe.getMessage());
		}
	}	

	@Test
	public void canAdd() throws Exception{	
		int size_before = db.count(doc);
		Apple apple = new Apple("Red","4");
		db.add(apple,doc);		
		assertEquals("Storage for <"+doc+"> should be one size bigger",size_before+1, db.count(doc));
	}

	@Test
	public void canCreate() throws Exception{			
		ODBFacade db = new ODBFacade("plocal:tests/com/terasoft/odb/db_fake", "admin", "admin");
		assertFalse("Db shldn't exist",db.exists());
		db.create();
		assertTrue("Db shld exist",db.exists());	
		db.drop();
		assertFalse("Db shldn't exist",db.exists());
	}
	
	
	@Test
	public void canRead() throws Exception{
		int size_count = db.count(doc);
		@SuppressWarnings("unchecked")
		int size_docs = ((List<ODocument>)db.read(doc, IO_TYP.READ_STORAGE_LIST_DOCS, null)).size();
		assertEquals("Count size should be the same as Doc size",size_count, size_docs);
	}

	@Test
	public void canRead_returnsSingleObj() throws Exception{		
		Apple apple = new Apple("Red", "4");
		String id = db.add(apple, doc);		
		assertEquals(apple.getColor(), ((Apple)db.read(id, IO_TYP.READ_ID_CLASS_OBJ, Apple.class)).getColor());
	}

	@Test
	public void canRead_returnsSingleObj_JSON() throws Exception{		
		Apple apple = new Apple("Red", "4");		
		String id = db.add(apple, doc);		
		assertEquals(
						JSON.writeValueAsString(apple), 
						JSON.writeValueAsString(JSON.readValue(((String)db.read(id, IO_TYP.READ_ID_JSON_OBJ)), Apple.class))
					);
	}
	
	@Test
	public void canRead_throwsExceptionWithUnsupportedRtrnTyp() throws Exception{
		
		IO_TYP typ = null;
		
		try{
			db.read("", typ, null);
			fail("Read should no support IO_TYP <"+typ+">");
		}catch(Unsupported_IO_TYP_Exception ure)
		{
			assertEquals("No support for IO_TYP <"+typ+">", ure.getMessage());
		}
	}
	
	@Test
	public void canUpd8() throws Exception{
		
		String oldColor = "Orange";
		String oldAge = "4";
		
		String newColor = "Red";
		String newAge = "1";
		
		String id = db.add(new Apple(oldColor, oldAge), doc);
		
		
		assertEquals(oldColor, ((Apple)db.read(id, IO_TYP.READ_ID_CLASS_OBJ, Apple.class)).getColor());
		assertEquals(oldAge, ((Apple)db.read(id, IO_TYP.READ_ID_CLASS_OBJ, Apple.class)).getAge());
		
		db.upd8(id, new Apple(newColor, newAge), IO_TYP.UPD8_ID);

		assertEquals(newColor, ((Apple)db.read(id, IO_TYP.READ_ID_CLASS_OBJ, Apple.class)).getColor());
		assertEquals(newAge, ((Apple)db.read(id, IO_TYP.READ_ID_CLASS_OBJ, Apple.class)).getAge());
		
	}

	@Test
	public void canDelete() throws Exception{
		
		String id = db.add(new Apple("Red", "15"), doc);
		
		int size_count_before = db.count(doc);
		db.del(id,IO_TYP.DEL_ID);
		
		int size_count_after = db.count(doc);
		assertEquals("Storage for <"+doc+"> should be one size smaller",size_count_before-1, size_count_after);	
	}
	
	@Test
	public void canDelete_withListOfIds() throws Exception{
		
		List<String> ids = Arrays.asList(new String[]{ db.add(new Apple("Red", "15"), doc), db.add(new Apple("Red", "15"), doc) });
		
		int size_count_before = db.count(doc);
		db.del(ids,IO_TYP.DEL_IDS);
		
		int size_count_after = db.count(doc);
		assertEquals("Storage for <"+doc+"> should be smaller ",size_count_before-ids.size(), size_count_after);	
	}
	
	@Test
	public void printStore_nonExistingStoreReturnsEmptyString() throws Exception{
		String result = db.printStore("Non Existant Store");
		assertEquals("Non existant store should return empty string", "", result);
	}
}
