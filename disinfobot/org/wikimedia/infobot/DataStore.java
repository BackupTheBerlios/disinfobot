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
 * $Id: DataStore.java,v 1.2 2004/12/20 07:28:26 kate Exp $
 */

package org.wikimedia.infobot;

import java.io.File;
import java.io.IOException;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.je.BtreeStats;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.util.RuntimeExceptionWrapper;

public abstract class DataStore {
	static protected Environment			dbenv;
	protected Database				db;
	protected StoredClassCatalog	catalog;
	
	public DataStore(String dbname) throws IOException, ClassNotFoundException, DatabaseException {
		if (dbenv == null) {
			EnvironmentConfig envconfig = new EnvironmentConfig();
			envconfig.setAllowCreate(true);
			envconfig.setTransactional(true);
			dbenv = new Environment(new File("db"), envconfig);
		}
		DatabaseConfig dbconfig = new DatabaseConfig();
		dbconfig.setAllowCreate(true);
		dbconfig.setTransactional(true);
		Transaction txn = dbenv.beginTransaction(null, null);
		db = dbenv.openDatabase(txn, dbname, dbconfig);
		txn.commit();
		catalog = new StoredClassCatalog(db);
	}

	public void finalize() throws DatabaseException {
		if (db != null) {
			db.close();
		}

		if (dbenv != null) {
			dbenv.sync();
			dbenv.close();
		}
	}

	public void storeItem(String key, Object value, Class type) {
		DatabaseEntry dbkey;
		DatabaseEntry data;
		EntryBinding binding;
		Transaction txn;
		
		try {
			dbkey = new DatabaseEntry(key.getBytes("UTF-8"));
			data = new DatabaseEntry();
			binding = new SerialBinding(catalog, type);
			binding.objectToEntry(value, data);
			txn = dbenv.beginTransaction(null, null);
			txn.setName("DataStore.storeItem for key=["+key+"] value=["
					+ value.toString() + "] type=" + type.toString());
			db.put(txn, dbkey, data);
			txn.commit();
		} catch (RuntimeExceptionWrapper e) {
			Infobot.logMsg("Unexpected runtime exception writing database entry");
			Infobot.logStackTrace(e);
			if (e.getCause() != null)
				Infobot.logStackTrace(e.getCause());
		} catch (Exception e) {
			Infobot.logMsg("Unexpected exception writing database entry");
			Infobot.logStackTrace(e);
		}
	}
	
	public Object getItem(String key, Class type) {
		Transaction txn;
		try {
			DatabaseEntry dbkey = new DatabaseEntry(key.getBytes("UTF-8"));
			DatabaseEntry data = new DatabaseEntry();
			txn = dbenv.beginTransaction(null, null);
			txn.setName("DataStore.getItem for key=["+key+"] type="
					+ type.toString());
			OperationStatus status = db.get(txn, dbkey, data, LockMode.DEFAULT);
			txn.commit();
			if (status == OperationStatus.NOTFOUND)
				return null;
			EntryBinding binding = new SerialBinding(catalog, type);
			return binding.entryToObject(data);
		} catch (Exception e) {
			Infobot.logMsg("Unexpected exception reading database entry");
			Infobot.logStackTrace(e);
			return null;
		}
	}
	
	public void eraseItem(String key) {
		Transaction txn;
		try {
			DatabaseEntry dbkey = new DatabaseEntry(key.getBytes("UTF-8"));
			txn = dbenv.beginTransaction(null, null);
			txn.setName("DataStore.eraseItem for key=["+key+"]");
			OperationStatus status = db.delete(txn, dbkey);
			txn.commit();
		} catch (Exception e) {
			Infobot.logMsg("Unexpected exception while removing database entry");
			Infobot.logStackTrace(e);
		}
	}
	
	public boolean keyExists(String key) {
		Transaction txn;
		try {
			DatabaseEntry dbkey = new DatabaseEntry(key.getBytes("UTF-8"));
			DatabaseEntry data = new DatabaseEntry();
			txn = dbenv.beginTransaction(null, null);
			txn.setName("DataStore.keyExists for key=["+key+"]");
			OperationStatus status = db.get(txn, dbkey, data, LockMode.DEFAULT);
			txn.commit();
			if (status == OperationStatus.NOTFOUND)
				return false;
			return true;
		} catch (Exception e) {
			Infobot.logMsg("Unexpected exception reading database entry");
			Infobot.logStackTrace(e);
			return false;
		}
	}
	
	public int numKeys() {
		try {
			StatsConfig s = new StatsConfig();
			BtreeStats stats = (BtreeStats) db.getStats(s);
			return (int) stats.getLeafNodeCount();
		} catch (Exception e) {
			Infobot.logMsg("Unexpected exception reading number of keys");
			Infobot.logStackTrace(e);
			return 0;
		}
	}
}
