package com.terasoft.terautils.odbfacade;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentPool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.terasoft.terautils.odbfacade.exceptions.NoDBFoundException;
import com.terasoft.terautils.odbfacade.exceptions.Unsupported_IO_TYP_Exception;
import com.terasoft.terautils.odbfacade.exceptions.Unsupported_SRCH_TYP_Exception;
import com.terasoft.terautils.odbfacade.exceptions.WrongCredException;
import com.terasoft.terautils.odbfacade.xmlmapping.Config;

public class ODBFacade {

	private String path, user, pass;
	private ODatabaseDocument db;
	private final ObjectMapper XML = new XmlMapper();
	private final ObjectMapper JSON = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		
	public enum IO_TYP{
		READ_STORAGE_LIST_DOCS,
		READ_STORAGE_LIST_OBJS,
		READ_STORAGE_JSON_OBJS,
		READ_ID_CLASS_OBJ,
		READ_ID_JSON_OBJ,
		UPD8_ID, 
		DEL_ID,
		DEL_IDS, 
		SRCH_STORAGE_LIST_DOCS, 
		SRCH_STORAGE_LIST_OBJS,
		SRCH_STORAGE_JSON_OBJS, READ_ID_DOC_OBJ
	}
	
	public enum SRCH_TYP{
		LK,EQ,HD,
		TL,GT,LT
	}
	
	public ODBFacade(String cfgPath) {
		super();
		load(cfgPath);
	}

	public ODBFacade() {
	}
	
	public ODBFacade(String path, String user, String pass) {
		super();
		this.path = path;
		this.user = user;
		this.pass = pass;
	}

	private void load(String path) {			
		Config cfg = null;
		try {
			cfg = XML.readValue(new File(path), Config.class);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.path = cfg.getPath().trim();
		this.pass = cfg.getPass().trim();
		this.user = cfg.getUser().trim();		
	}

	
	public ODatabaseDocument getDb() {
		return db;
	}

	public void setDb(ODatabaseDocument db) {
		this.db = db;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String name) {
		this.path = name;
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

	public boolean canConn() {
		
		try{
			ODatabaseDocumentTx tx = ODatabaseDocumentPool.global().acquire(path, user, pass);		
			boolean result = tx.exists();
			tx.close();
			return result;
		}catch(OStorageException ose)
		{
			throw new NoDBFoundException("No db found on path <"+path+">");
		}catch(OSecurityAccessException osae)
		{
			throw new WrongCredException("Wong username or password for db found on path <"+path+">");
		}
		
	}

	public boolean exists()
	{
		try{
			return canConn();
		}catch(NoDBFoundException ndbfe ){
			return false;
		}
	}
	
	private boolean exists(ODatabaseDocumentTx tx) {
		return tx.exists();
	}	
	
	public void create() {
		ODatabaseDocumentTx tx = new ODatabaseDocumentTx(path);	
		
		if( !exists(tx) ){
			tx.create();
		}		
		
		tx.close();
	}

	public void drop() {
		ODatabaseDocumentTx tx = ODatabaseDocumentPool.global().acquire(path, user, pass);				
		
		if(tx.isClosed()){
			tx.open(user, pass);
		}
		
		tx.drop();
		tx.close();
	}

	public int count(String storage) {
		db = ODatabaseDocumentPool.global().acquire(path, user, pass); 
		int result = 0;
		
		try{
			result = (int)db.countClass(storage);
		}catch(IllegalArgumentException iae)
		{}
		
		db.close();
		return result;
	}

	public List<String> classNames()
	{
		db = ODatabaseDocumentPool.global().acquire(path, user, pass);
		List<String> stores = new ArrayList<String>();
		for(OClass store : db.getMetadata().getSchema().getClasses())
		{
			stores.add(store.getName());
		}
		return stores;
	}
	
	public synchronized String add(Object data, String storage) {
		String result = "";
		String in = "";
		try {
			in = JSON.writeValueAsString(data);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		db = ODatabaseDocumentPool.global().acquire(path, user, pass);	
		ODatabaseRecordThreadLocal.INSTANCE.set(db);
		ODocument ref = new ODocument(storage);
		ref.fromJSON(in);
		ref.save();
		result = ref.field("@rid")+"";
		db.close();
		return result;		
	}
	
	public synchronized boolean addStore(String storage)
	{
		db = ODatabaseDocumentPool.global().acquire(path, user, pass);	
		ODatabaseRecordThreadLocal.INSTANCE.set(db);
		
		db.getMetadata().getSchema().createClass(storage);
		
		db.close();
		return true;
	}
	
	public synchronized boolean hasField(String id, String field)
	{
		boolean result = false;
		db = ODatabaseDocumentPool.global().acquire(path, user, pass);	
		ODatabaseRecordThreadLocal.INSTANCE.set(db);

		ODocument doc = readDocFromId(id, db);
		
		result = doc.containsField(field);
		
		db.close();
		return result;
	}
	
	public synchronized boolean setField(String id, String field, Object value)
	{
		db = ODatabaseDocumentPool.global().acquire(path, user, pass);	
		ODatabaseRecordThreadLocal.INSTANCE.set(db);

		ODocument doc = readDocFromId(id, db);
		
		doc.field(field, value);
		doc.save();
		
		db.close();
		return true;
	}
	
	public synchronized boolean setField(String id, Map<String, Object> fields)
	{
		db = ODatabaseDocumentPool.global().acquire(path, user, pass);	
		ODatabaseRecordThreadLocal.INSTANCE.set(db);

		ODocument doc = readDocFromId(id, db);
		
		for(Entry<String, Object> entry : fields.entrySet()) {
		    String name = entry.getKey();
		    Object value = entry.getValue();
			doc.field(name, value);
		}		
		
		doc.save();
		
		db.close();
		return true;
	}

	public synchronized boolean replaceDoc(String id, Map<String, Object> fields)
	{
		db = ODatabaseDocumentPool.global().acquire(path, user, pass);	
		ODatabaseRecordThreadLocal.INSTANCE.set(db);

		ODocument doc = readDocFromId(id, db);
		doc.clear();
		
		for(Entry<String, Object> entry : fields.entrySet()) {
		    String name = entry.getKey();
		    Object value = entry.getValue();
			doc.field(name, value);
		}		
		
		doc.save();
		
		db.close();
		return true;
	}
	
	
	
	public synchronized boolean hasStore(String storage)
	{
		db = ODatabaseDocumentPool.global().acquire(path, user, pass);	
		ODatabaseRecordThreadLocal.INSTANCE.set(db);
		
		boolean result = db.getMetadata().getSchema().existsClass(storage);
		
		db.close();
		return result;
	}
	
	public synchronized boolean hasDoc(String id)
	{
		db = ODatabaseDocumentPool.global().acquire(path, user, pass);	
		ODatabaseRecordThreadLocal.INSTANCE.set(db);
		
		ODocument doc = readDocFromId(id, db);
		
		db.close();
		return !(doc == null);
	}
	
	public synchronized boolean delStore(String storage)
	{
		db = ODatabaseDocumentPool.global().acquire(path, user, pass);	
		ODatabaseRecordThreadLocal.INSTANCE.set(db);
		
		db.getMetadata().getSchema().dropClass(storage);
		
		db.close();
		return true;
	}	
	
	private boolean IsDefaultStore(String store)
	{
		return 	store.equals("OTriggered") ||
				store.equals("OIdentity") ||
				store.equals("OSchedule") ||
				store.equals("ORole") ||
				store.equals("OUser") ||
				store.equals("ORIDs") ||
				store.equals("OFunction") ||
				store.equals("ORestricted"); 
	}	
	
	public synchronized boolean delAllStores()
	{
		db = ODatabaseDocumentPool.global().acquire(path, user, pass);	
		ODatabaseRecordThreadLocal.INSTANCE.set(db);
		
		for(OClass _class : db.getMetadata().getSchema().getClasses())
		{
			if(!IsDefaultStore(_class.getName()))
				db.getMetadata().getSchema().dropClass(_class.getName());
		}
		
		db.close();
		return true;
	}		
	
	public synchronized boolean delAllRecords(String storage)
	{
		db = ODatabaseDocumentPool.global().acquire(path, user, pass);	
		ODatabaseRecordThreadLocal.INSTANCE.set(db);
		
		db.getMetadata().getSchema().dropClass(storage);
		db.getMetadata().getSchema().createClass(storage);
		
		db.close();
		return true;
	}		
	
	
	public synchronized String addJSON(String dataJSON, String storage) {
		String result = "";		
		db = ODatabaseDocumentPool.global().acquire(path, user, pass);	
		ODatabaseRecordThreadLocal.INSTANCE.set(db);
		ODocument ref = new ODocument(storage);
		ref.fromJSON(dataJSON);
		ref.save();
		result = ref.field("@rid")+"";
		db.close();
		return result;		
	}	

	public <T> Object read(String what, String flag)
	{		
		return read(what, IO_TYP.valueOf(flag), null);
	}
		
	public <T> Object read(String what, IO_TYP flag)
	{
		return read(what, flag, null);
	}
	
	public <T> Object read(String what, IO_TYP flag, Class<T> _class)
	{
		if( flag == null )
		{
			throw new Unsupported_IO_TYP_Exception("No support for IO_TYP <"+flag+">");
		}
		
		db = ODatabaseDocumentPool.global().acquire(path, user, pass);
		Object result;
		
		switch (flag) 
		{
			case READ_STORAGE_LIST_DOCS:				
				result = readClass(what, db);
				db.close();
				return result;

			case READ_STORAGE_LIST_OBJS:				
				result = readObjsFromClass(what, db, _class);
				db.close();
				return result;
				
			case READ_STORAGE_JSON_OBJS:				
				result = readObjsFromClass_JSON(what, db);
				db.close();
				return result;
								
			case READ_ID_CLASS_OBJ:	
				result = readObjFromId(what, db, _class);
				db.close();
				return result;
				
			case READ_ID_JSON_OBJ:	
				result = readObjFromId_JSON(what, db);
				db.close();
				return result;
				
			case READ_ID_DOC_OBJ:	
				result = readDocFromId(what, db);
				db.close();
				return result;				
				
			default:
				db.close();
				throw new Unsupported_IO_TYP_Exception("No support for IO_TYP <"+flag+">");	
			
		}
		
	}
		
	private List<ODocument> readClass(String store, ODatabaseDocument db)
	{
		List<ODocument> result = new ArrayList<ODocument>();
		for(ODocument doc : db.browseClass(store))
		{
			result.add(doc);
		}
		return result;
	}
	
	private <T> List<T> readObjsFromClass(String store, ODatabaseDocument db, Class<T> classOfT)
	{
		List<T> result = new ArrayList<T>();
		for(ODocument doc : db.browseClass(store))
		{
			try {
				result.add(JSON.readValue(doc.toJSON(),classOfT));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return result;		
	}

	private String readObjsFromClass_JSON(String store, ODatabaseDocument db)
	{
		String result = "";
		result += "[";
		int brak = 0;	
		for(ODocument doc : db.browseClass(store))
		{
			if(brak > 0)
			{
				result += ", ";
			}		

			result += doc.toJSON();
			brak++;
		}
		result += "]";		
		return result;		
	}
		
	private <T> Object readObjFromId(String id, ODatabaseDocument db, Class<T> classOfT)
	{
		String in = readDocFromId(id, db).toJSON();		
		try {
			return JSON.readValue(in, classOfT);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	private <T> Object readObjFromId_JSON(String id, ODatabaseDocument db)
	{
		return readDocFromId(id, db).toJSON();
	}	
	
	@SuppressWarnings("unchecked")
	private ODocument readDocFromId(String id, ODatabaseDocument db)
	{
		ODocument result = null;
		

		try{
		result = (
								(List<ODocument>)
								db.query( new OSQLSynchQuery<ODocument>
											("select from "+id))
							).get(0);
		}catch(IndexOutOfBoundsException iobe){
			
		}
		
		return result;
	}
		
	public void upd8(String what, Object data, IO_TYP flag)
	{
		db = ODatabaseDocumentPool.global().acquire(path, user, pass);
		switch(flag)
		{
			case UPD8_ID:
				upd8_ID(what, data, db);
				db.close();
			break;
		
			default:
				db.close();
				throw new Unsupported_IO_TYP_Exception("No support for IO_TYP <"+flag+">");			
		}
	}
	
	private void upd8_ID(String id, Object newData, ODatabaseDocument db)
	{
		ODocument oldData = readDocFromId(id, db);
		Field[] params = newData.getClass().getDeclaredFields();
		for(Field param : params)
		{
			String param_name = param.getName();
			Object param_value = getFieldVal(param,newData);
			
			if( param_value == null )
			{
				continue;
			}
			
			oldData.field(param_name, param_value);
		}
		oldData.save();
	}

	private Object getFieldVal(Field field, Object data)
	{
		Object result = "";
		try {
			field.setAccessible(true);
			result = field.get(data);
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		return result;
	}

	public void del(String id)
	{
		del(id, IO_TYP.DEL_ID);
	}
	
	@SuppressWarnings("unchecked")
	public void del(Object what, IO_TYP flag) {
		db = ODatabaseDocumentPool.global().acquire(path, user, pass);
		switch(flag)
		{
			case DEL_ID:
				del_ID((String)what,db);
				db.close();
			break;
			
			case DEL_IDS:
				del_IDS((Iterable<String>)what,db);
				db.close();
			break;
		
			default:
				db.close();
				throw new Unsupported_IO_TYP_Exception("No support for IO_TYP <"+flag+">");			
		}		
	}
	
	private void del_ID(String id, ODatabaseDocument db)
	{
		readDocFromId(id, db).delete();
	}
	
	private void del_IDS(Iterable<String> ids, ODatabaseDocument db){
		for(String id : ids)
		{
			del_ID(id, db);
		}
	}

	public List<ODocument> search(	String store, String field,
							String value	){
		return search(store, buildFilter(field, value, SRCH_TYP.LK));
	}
	
	public List<ODocument> search(	String store, String field, 
									String value, SRCH_TYP flag_srch) {		
		return search(store, buildFilter(field, value, flag_srch));
	}	
	
	public Object search(	String store, String field,
							String value, Class<?> _classOfStore	){				
		return search(store, buildFilter(field, value, SRCH_TYP.LK), _classOfStore);
	}	

	public Object search(	String store, String field, 
							String value, Class<?> _classOfStore,
							SRCH_TYP flag_srch) {		
		return search(store, buildFilter(field, value, flag_srch), _classOfStore);
	}	
		
	@SuppressWarnings("unchecked")
	public List<ODocument> search(	String store,
									Map<SRCH_TYP, Map<String, String>> filters ){
		return (List<ODocument>) search(store, filters, null, IO_TYP.SRCH_STORAGE_LIST_DOCS);
	}
	
	public Object search(	String store,
									Map<SRCH_TYP, Map<String, String>> filters, 
									Class<?> _classOfStore){
		return search(store, filters, _classOfStore, IO_TYP.SRCH_STORAGE_LIST_OBJS);
	}
		
	public Object search(	String store,
							Map<SRCH_TYP, Map<String, String>> filters, 
							Class<?> _classOfStore,
							IO_TYP flag_io) {
		
		if( flag_io == null ){
			throw new Unsupported_IO_TYP_Exception("No support for IO_TYP <"+flag_io+">");
		}
		
		db = ODatabaseDocumentPool.global().acquire(path, user, pass);
		Object result = null;
		switch (flag_io) 
		{
			case SRCH_STORAGE_LIST_DOCS:
				result = docSearch(store, filters, db);
				db.close();
				return result;
				
			case SRCH_STORAGE_LIST_OBJS:
				result = objSearch(store, filters, _classOfStore, db);
				db.close();
				return result;
				
			default:
				db.close();
				throw new Unsupported_IO_TYP_Exception("No support for IO_TYP <"+flag_io+">");				
		}
			
	}
		
	private List<ODocument> docSearch(	String store,
										Map<SRCH_TYP, Map<String, String>> filters, 
										ODatabaseDocument db) {
		
		String query = buildQuery(store, filters, db);
		List<ODocument> result = new ArrayList<ODocument>();		
		result = execQuery(query, db);
		return result;
	}
	
	private Object objSearch(	String store,
								Map<SRCH_TYP, Map<String, String>> filters,
								Class<?> _classOfStore, 
								ODatabaseDocument db){

		List<ODocument> docs = docSearch(store, filters, db);		
		List<Object> result = new ArrayList<Object>();
		for(ODocument doc : docs)
		{
			try {
				result.add(JSON.readValue(doc.toJSON(), _classOfStore));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return result;	
	}
		
	private String buildQuery( String store, Map<SRCH_TYP, Map<String, String>> filters, ODatabaseDocument db)
	{
		String result = "select from "+store+" ";
		
		int filter_count = 0;
		
		if(filters == null)
		{
			return "";
		}
		
		for (Entry<SRCH_TYP, Map<String, String>> filter : filters.entrySet()) {
			
			if(filter_count == 0)
			{
				result += "where ";
			}
			
			SRCH_TYP flag_srch = filter.getKey();
			Map<String, String> fields = filter.getValue();
			
			int field_count = 0;
			
			switch (flag_srch){
				case LK:
					for( Entry<String, String> field : fields.entrySet() ){
						if(field_count > 0){
							result += "and ";
						}						
						result += field.getKey()+" like '%"+field.getValue()+"%' ";						
						field_count++;
					}
				break;	
				
				case EQ:
					for( Entry<String, String> field : fields.entrySet() ){
						if(field_count > 0){
							result += "and ";
						}						
						result += field.getKey()+" = '"+field.getValue()+"' ";						
						field_count++;
					}
				break;	
				

				default:
					db.close();
					throw new Unsupported_SRCH_TYP_Exception("No support for SRCH_TYP <"+flag_srch+">");				
			}
			
			filter_count++;
		}
		
		return result;
	}
	
	private Map<SRCH_TYP, Map<String, String>> buildFilter(String field, String value, SRCH_TYP flag_srch)
	{	
		Map<SRCH_TYP, Map<String, String>> result = new HashMap<ODBFacade.SRCH_TYP, Map<String,String>>();
		Map<String, String> props = new HashMap<String, String>();
		
		props.put(field, value);
		result.put(flag_srch, props);
		
		return result;
	}
		
	public List<ODocument> execQuery(String query)
	{
		db = ODatabaseDocumentPool.global().acquire(path, user, pass);
		List<ODocument> result = execQuery(query, db);
		db.close();
		return result;
	}
	
	private List<ODocument> execQuery(String query, ODatabaseDocument db){
		
		List<ODocument> result = new ArrayList<ODocument>();
		
		try{
		result = db.query( new OSQLSynchQuery<ODocument>(query)); 
		}
		catch(OQueryParsingException oqpe){}
		
		return result;
	}

	public String printStore(String store)
	{
		String result = "";
		db = ODatabaseDocumentPool.global().acquire(path, user, pass);
		
		try{
			for(ODocument doc : db.browseClass(store))
			{
				result += doc.toJSON() + "\n";
			}
		}
		catch(IllegalArgumentException iae){}
		
		db.close();
		return result;
	}
	
	public void printToFile(String store, String file)
	{
		db = ODatabaseDocumentPool.global().acquire(path, user, pass);
		
		try{
			for(ODocument doc : db.browseClass(store))
			{
				try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(file, true)))) {
				    out.println(doc.toJSON() + "\n");
				}catch (IOException e) {
				    //exception handling left as an exercise for the reader
				}
			}
		}
		catch(IllegalArgumentException iae){}
		
		db.close();
	}
	
}
