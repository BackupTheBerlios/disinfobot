/*
 * Copyright 2004 Kate Turner
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy 
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights 
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell 
 * copies of the Software, and to permit persons to whom the Software is 
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in 
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR 
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE 
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 * 
 * $Id: FactoidDataStore.java,v 1.2 2004/11/24 12:18:53 kate Exp $
 */
package org.wikimedia.infobot;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

/**
 * @author Kate Turner
 *
 */
public class FactoidDataStore extends DataStore {
	private final int ST_KEY = 1, ST_VAL = 2, ST_AUTHOR = 3;
	
	/**
	 * @param storefile
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws DatabaseException
	 */
	public FactoidDataStore() 
	throws IOException, ClassNotFoundException, DatabaseException {
		super("factoids");
	}

	public void importAncientDatabase(String file) 
	throws ClassNotFoundException, IOException {
		Infobot.logMsg("Importing ancient database...");
		FileInputStream fis = new FileInputStream(file);
		ObjectInputStream ois = new ObjectInputStream(fis);
		int version = 1;
		try {
			Integer ver = (Integer)ois.readObject();
			version = ver.intValue();
			if (version != 2) {
				Infobot.logMsg("I don't understand how to read this version " + version
						+ " database");
				throw new Exception("Database unrecognised");
			}
		} catch (Exception e) {
			Infobot.logMsg("This looks like an obsolete version 1 database, I can't read it.");
			return;
		}
		int numkeys = ((Integer)ois.readObject()).intValue();
		while (numkeys-- > 0) {
			String name = (String) ois.readObject();
			String value = (String) ois.readObject();
			String author = (String) ois.readObject();
			Integer queries = (Integer) ois.readObject();
			
			Factoid f = new Factoid();
			f.setText(value);
			f.setAuthor(author);
			f.setNumQueries(queries.intValue());
			storeItem(name, f);
		}
		ois.close();
	}

	public void storeItem(String key, Factoid value) {
		storeItem(key, value, Factoid.class);
	}
	
	public Factoid getItem(String key) {
		return (Factoid) getItem(key, Factoid.class);
	}
	
	public List searchKeys(String term) throws UnsupportedEncodingException {
		return searchSomething(term, ST_KEY);
	}

	public List searchVals(String term) throws UnsupportedEncodingException {
		return searchSomething(term, ST_VAL);
	}
	
	public List getForAuthor(String who) throws UnsupportedEncodingException {
		return searchSomething(who, ST_AUTHOR);
	}
	
	public List searchSomething(String term, int searchwhat) 
	throws UnsupportedEncodingException {
		List r = new ArrayList();
		Cursor cursor = null;
		EntryBinding binding = new SerialBinding(catalog, Factoid.class);

		try {
			cursor = db.openCursor(null, null);
			DatabaseEntry key = new DatabaseEntry(), 
				data = new DatabaseEntry();
		
			while (cursor.getNext(key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
				Factoid f;
				try {
					f = (Factoid) binding.entryToObject(data);
				} catch (Exception e) {
					continue;
				}
				String n = new String(key.getData(), "UTF-8");
				String searchthis = "";
				switch(searchwhat) {
				case ST_KEY: {
					searchthis = n;
					break;
				}
				case ST_VAL: {
					searchthis = f.getText();
					break;
				}
				case ST_AUTHOR: {
					searchthis = f.getAuthor();
					break;
				}
				}
				if (searchwhat == ST_AUTHOR) {
					if (searchthis.equals(term))
						r.add(n);
				} else {
					Pattern p = Pattern.compile(term.toLowerCase());
					Matcher m = p.matcher(searchthis.toLowerCase());
					if (m.find())
						r.add(n);
				}
			}
		
			cursor.close();
		} catch (DatabaseException e) {
			Infobot.logMsg("Unexpected exception reading database");
			Infobot.logStackTrace(e);
			return null;
		} catch (PatternSyntaxException pse) {
			if (cursor != null)
				try { cursor.close(); } catch (Exception q) {}
			cursor = null;
			throw pse;
		} finally {
			if (cursor != null) {
				try {
					cursor.close();
				} catch (Exception f) {}
			}
		}
		return r;
	}
}
