/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: DbTree.java,v 1.1 2004/11/22 18:27:59 kate Exp $
*/

package com.sleepycat.je.dbi;

import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.VerifyConfig;
import com.sleepycat.je.dbi.CursorImpl.SearchMode;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogException;
import com.sleepycat.je.log.LogReadable;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.log.LoggableObject;
import com.sleepycat.je.tree.ChildReference;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.tree.Key;
import com.sleepycat.je.tree.LN;
import com.sleepycat.je.tree.MapLN;
import com.sleepycat.je.tree.NameLN;
import com.sleepycat.je.tree.Tree;
import com.sleepycat.je.tree.TreeUtils;
import com.sleepycat.je.tree.WithRootLatched;
import com.sleepycat.je.txn.AutoTxn;
import com.sleepycat.je.txn.BasicLocker;
import com.sleepycat.je.txn.Locker;
import com.sleepycat.je.utilint.DbLsn;

/**
 * Represents the DatabaseImpl Naming Tree.
 */
public class DbTree implements LoggableObject, LogReadable {

    /* The id->DatabaseImpl tree is always id 0 */
    public static final DatabaseId ID_DB_ID = new DatabaseId(0);
    /* The name->id tree is always id 1 */
    public static final DatabaseId NAME_DB_ID = new DatabaseId(1);

    /* Names of the mapping database. */
    public static final String ID_DB_NAME = "_jeIdMap";
    public static final String NAME_DB_NAME = "_jeNameMap";
    public static final String UTILIZATION_DB_NAME = "_jeUtilization";

    /* Reserved database names. */
    private static final String[] RESERVED_DB_NAMES = {
        ID_DB_NAME,
        NAME_DB_NAME,
        UTILIZATION_DB_NAME,
    };

    /* Database id counter, must be accessed w/synchronization. */
    private int lastAllocatedDbId;        

    private DatabaseImpl idDatabase;          // map db ids -> databases
    private DatabaseImpl nameDatabase;        // map names -> dbIds
    private EnvironmentImpl envImpl; 

    /**
     * Create a dbTree from the log.
     */
    public DbTree()
        throws DatabaseException {
        	
        this.envImpl = null;
        idDatabase = new DatabaseImpl();
        nameDatabase = new DatabaseImpl();
    }

    /**
     * Create a new dbTree for a new environment.
     */
    public DbTree(EnvironmentImpl env)
        throws DatabaseException {

        this.envImpl = env;
        idDatabase = new DatabaseImpl(ID_DB_NAME,
				      new DatabaseId(0),
				      env,
				      new DatabaseConfig());
                                  
        nameDatabase = new DatabaseImpl(NAME_DB_NAME,
					new DatabaseId(1),
					env,
					new DatabaseConfig());
                                  
        lastAllocatedDbId = 1;
    }

    /**
     * Get the latest allocated id, for checkpoint info.
     */
    public synchronized int getLastDbId() {
        return lastAllocatedDbId;
    }

    /**
     * Get the next available database id.
     */
    private synchronized int getNextDbId() {
        return ++lastAllocatedDbId;
    }

    /**
     * Initialize the db id, from recovery.
     */
    public synchronized void setLastDbId(int maxDbId) {
        lastAllocatedDbId = maxDbId; 
    }

    /**
     * Set the db environment during recovery, after instantiating the tree
     * from the log.
     */
    void setEnvironmentImpl(EnvironmentImpl envImpl)
        throws DatabaseException {

        this.envImpl = envImpl;
        idDatabase.setEnvironmentImpl(envImpl);
        nameDatabase.setEnvironmentImpl(envImpl);
    }

    /**
     * Create a database.
     * @param locker owning locker
     * @param databaseName identifier for database
     * @param dbConfig
     */
    public synchronized DatabaseImpl createDb(Locker locker, 
                                              String databaseName,
                                              DatabaseConfig dbConfig,
                                              Database databaseHandle)
        throws DatabaseException {

        /* Create a new database object. */
        DatabaseId newId = new DatabaseId(getNextDbId());
        DatabaseImpl newDb = new DatabaseImpl(databaseName,
					      newId,
					      envImpl,
					      dbConfig);
        CursorImpl idCursor = null;
        CursorImpl nameCursor = null;
        boolean operationOk = false;
        Locker autoTxn = null;
        try {
            /* Insert it into name -> id db. */
            nameCursor = new CursorImpl(nameDatabase, locker);
            LN nameLN = new NameLN(newId);
            nameCursor.putLN(new Key(databaseName.getBytes("UTF-8")),
			     nameLN, false);

            /* 
             * If this is a non-handle use, no need to record any handle locks.
             */
            if (databaseHandle != null) {
                locker.addToHandleMaps(new Long(nameLN.getNodeId()),
                                       databaseHandle);
            }

            /* Insert it into id -> name db, in auto commit mode. */
            autoTxn = new AutoTxn(envImpl, new TransactionConfig());
            idCursor = new CursorImpl(idDatabase, autoTxn);
            idCursor.putLN(new Key(newId.getBytes()), new MapLN(newDb), false);
            operationOk = true;
	} catch (UnsupportedEncodingException UEE) {
	    throw new DatabaseException(UEE);
        } finally {
            if (idCursor != null) {
                idCursor.close();
            }
 
            if (nameCursor != null) {
                nameCursor.close();
            }

            if (autoTxn != null) {
                autoTxn.operationEnd(operationOk);
            }
        }

        return newDb;
    }

    /**
     * Called by the Tree to propagate a root change.  If the tree is a data
     * database, we will write the MapLn that represents this db to the log. If
     * the tree is one of the mapping dbs, we'll write the dbtree to the log.
     *
     * @param db the target db
     */
    public void modifyDbRoot(DatabaseImpl db)
        throws DatabaseException {
        
        if (db.getId().equals(ID_DB_ID) ||
            db.getId().equals(NAME_DB_ID)) {
            envImpl.logMapTreeRoot();
        } else {
            Locker locker = new AutoTxn(envImpl, new TransactionConfig());
            CursorImpl cursor = new CursorImpl(idDatabase, locker);
            boolean operationOk = false;
            try {
                DatabaseEntry keyDbt =
		    new DatabaseEntry(db.getId().getBytes());
		MapLN mapLN = null;
                try {
                    boolean searchOk = (cursor.searchAndPosition
					(keyDbt, new DatabaseEntry(),
					 SearchMode.SET, LockMode.RMW) &
			 CursorImpl.FOUND) != 0;
                    assert searchOk : "can't find database " + db.getId();

                    mapLN = (MapLN)
                        cursor.getCurrentLNAlreadyLatched(LockMode.DEFAULT);
                } finally {
                    cursor.releaseBINs();
                }

		RewriteMapLN writeMapLN = new RewriteMapLN(cursor);
		mapLN.getDatabase().getTree().withRootLatched(writeMapLN);

                operationOk = true;
            } finally {
                if (cursor != null) {
                    cursor.close();
                }

                locker.operationEnd(operationOk);
            }
        }
    }

    private static class RewriteMapLN implements WithRootLatched {
        private CursorImpl cursor;

        RewriteMapLN(CursorImpl cursor) {
            this.cursor = cursor;
        }

        /**
         * @return true if the in-memory root was replaced.
         */
        public IN doWork(ChildReference root) 
            throws DatabaseException {
            
	    DatabaseEntry dataDbt = new DatabaseEntry(new byte[0]);
	    cursor.putCurrent(dataDbt, null, null);
            return null;
        }
    }

    /**
     * Return true if the operation succeeded, false otherwise.
     */
    boolean dbRename(Locker locker, String databaseName, String newName)
        throws DatabaseException {

        DatabaseImpl db = getDb(locker, databaseName, null);
        if (db == null) {
            throw new DatabaseNotFoundException
                ("Attempted to rename non-existent database " + databaseName);
        }
        CursorImpl nameCursor = new CursorImpl(nameDatabase, locker);

        try {
            DatabaseEntry keyDbt =
                new DatabaseEntry(databaseName.getBytes("UTF-8"));
            boolean found =
                (nameCursor.searchAndPosition(keyDbt, null,
					      SearchMode.SET, null) &
		 CursorImpl.FOUND) != 0;
            if (!found) {
                return false;
            }

            /* Call getCurrentLN to write lock the nameLN. */
            nameCursor.getCurrentLNAlreadyLatched(LockMode.RMW);            

            /* 
             * Check the open handle count after we have the write lock and no
             * other transactions can open. XXX, another handle using the same
             * txn could open ...
             */
            int handleCount = db.getReferringHandleCount();
            if (handleCount > 0) {
                throw new DatabaseException("Can't rename database " +
					    databaseName + "," + handleCount + 
					    " open Dbs exist");
            }

            /* Delete oldName->dbId entry and put in a newName->dbId. */
            nameCursor.delete();
            nameCursor.putLN(new Key(newName.getBytes("UTF-8")),
                             new NameLN(db.getId()), false);
            return true;
	} catch (UnsupportedEncodingException UEE) {
	    throw new DatabaseException(UEE);
        } finally {
            nameCursor.releaseBIN();
            nameCursor.close();
        }
    }

    /**
     * Remove the database by deleting the name LN.
     */
    void dbRemove(Locker locker, String databaseName)
        throws DatabaseException {

        DatabaseImpl db = getDb(locker, databaseName, null);
        if (db == null) {
            throw new DatabaseNotFoundException
                ("Attempted to remove non-existent database " +
                 databaseName );
        }
        CursorImpl nameCursor = new CursorImpl(nameDatabase, locker);

        try {
            DatabaseEntry keyDbt =
                new DatabaseEntry(databaseName.getBytes("UTF-8"));
            boolean found =
                (nameCursor.searchAndPosition(keyDbt, null,
					      SearchMode.SET, null) &
		 CursorImpl.FOUND) != 0;
            if (!found) {
                return;
            }

            /* Call getCurrentLN to write lock the nameLN. */
            nameCursor.getCurrentLNAlreadyLatched(LockMode.RMW);

            /* 
             * Check the open handle count after we have the write lock and no
             * other transactions can open. XXX, another handle using the same
             * txn could open ...
             */
            int handleCount = db.getReferringHandleCount();
            if (handleCount > 0) {
                throw new DatabaseException("Can't remove database " +
					    databaseName + "," + handleCount + 
					    " open Dbs exist");
            }

            /*
             * Delete the NameLN. There's no need to mark any Database handle
             * invalid, because the handle must be closed when we take action
             * and any further use of the handle will re-look up the database.
             */
            nameCursor.delete();

            /* Record utilization profile changes for the deleted database. */
            db.recordObsoleteNodes();

            /* Schedule database for final deletion during commit. */
            locker.setIsDeletedAtCommit(db);
	} catch (UnsupportedEncodingException UEE) {
	    throw new DatabaseException(UEE);
        } finally {
            nameCursor.releaseBIN();
            nameCursor.close();
        }
    }

    /**
     * Truncate a database named by databaseName. Return the new DatabaseImpl
     * object that represents the truncated database.  The old one is marked as
     * deleted.
     */
    TruncateResult truncate(Locker locker, DatabaseImpl oldDatabase)
        throws DatabaseException {

        CursorImpl nameCursor = new CursorImpl(nameDatabase, locker);

        try {
            String databaseName = getDbName(oldDatabase.getId());
            DatabaseEntry keyDbt =
                new DatabaseEntry(databaseName.getBytes("UTF-8"));
            boolean found =
                (nameCursor.searchAndPosition(keyDbt, null,
					      SearchMode.SET, null) &
		 CursorImpl.FOUND) != 0;
            if (!found) {

                /* 
                 * Should be found, since truncate is instigated from
                 * Database.truncate();
                 */
                throw new DatabaseException
                    ("Database " + databaseName +  " not found in map tree");
            }

            /* Call getCurrentLN to write lock the nameLN. */
            NameLN nameLN = (NameLN)
                nameCursor.getCurrentLNAlreadyLatched(LockMode.RMW);

            /* 
             * Check the open handle count after we have the write lock and no
             * other transactions can open. XXX, another handle using the same
             * txn could open ...
             */
            int handleCount = oldDatabase.getReferringHandleCount();
            if (handleCount > 1) {
                throw new DatabaseException("Can't truncate database " +
					    databaseName + "," + handleCount + 
					    " open databases exist");
            }
            
            /*
             * Make a new database with an empty tree. Make the nameLN refer to
             * the id of the new database.
             */
            DatabaseImpl newDb;
            DatabaseId newId = new DatabaseId(getNextDbId());
	    newDb = (DatabaseImpl) oldDatabase.clone();
            newDb.setId(newId);
            newDb.setTree(new Tree(newDb));
            
            /* Insert the new db into id -> name map */
            CursorImpl idCursor = null; 
            try {
                idCursor = new CursorImpl(idDatabase, locker);
                idCursor.putLN(new Key(newId.getBytes()),
			       new MapLN(newDb), false);
            } finally {
                idCursor.close();
            }
            nameLN.setId(newDb.getId());

            /* Record utilization profile changes for the deleted database. */
            int recordCount = oldDatabase.recordObsoleteNodes();

            /* Schedule database for final deletion during commit. */
            locker.setIsDeletedAtCommit(oldDatabase);

            /* log the nameLN. */
            DatabaseEntry dataDbt = new DatabaseEntry(new byte[0]);
            nameCursor.putCurrent(dataDbt, null, null);
            return new TruncateResult(newDb, recordCount);
	} catch (CloneNotSupportedException CNSE) {
	    throw new DatabaseException(CNSE);
	} catch (UnsupportedEncodingException UEE) {
	    throw new DatabaseException(UEE);
        } finally {
            nameCursor.releaseBIN();
            nameCursor.close();
        }
    }

    /**
     * Get a database object given a database name.
     * @param nameLocker is used to access the NameLN. As always, a NullTxn
     *  is used to access the MapLN.
     * @param databaseName target database
     * @return null if database doesn't exist
     */
    public DatabaseImpl getDb(Locker nameLocker,
                              String databaseName,
                              Database databaseHandle)
        throws DatabaseException {

        BasicLocker locker = null;
        try {

            /* 
             * Search the nameDatabase tree for the NameLn for this name.
             * Release locks before searching the id tree
             */
            CursorImpl nameCursor = null;
            DatabaseId id = null;
            try {

                nameCursor = new CursorImpl(nameDatabase, nameLocker,
                                            true, null);
                DatabaseEntry keyDbt =
		    new DatabaseEntry(databaseName.getBytes("UTF-8"));
                boolean found =
                    (nameCursor.searchAndPosition(keyDbt, null,
						  SearchMode.SET, null) &
                     CursorImpl.FOUND) != 0;

                if (found) {
                    NameLN nameLN = (NameLN)
                        nameCursor.getCurrentLNAlreadyLatched
			(LockMode.DEFAULT);
                    id = nameLN.getId();

                    /* 
                     * If this is a non-handle use, no need to record any
                     * handle locks.
                     */
                    if (databaseHandle != null) {
                        nameLocker.addToHandleMaps(new Long(nameLN.getNodeId()),
                                                   databaseHandle);
                    }
                } 
            } finally {
                if (nameCursor != null) {
                    nameCursor.releaseBIN();
                    nameCursor.close();
                }
            }

            /*
             * Now search the id tree. Use a BasicLocker, we never want to read
             * lock the MapLN transactionally. Don't use nameLocker, because
             * that locker will be open for a long time, since it's being used
             * by a database operation.
             */
            locker = new BasicLocker(envImpl);
            if (id == null) {
                return null;
            } else {
                return getDb(locker, id);
            }
	} catch (UnsupportedEncodingException UEE) {
	    throw new DatabaseException(UEE);
        } finally {
            if (locker != null) {
                locker.operationEnd();
            }
        }
    }

    /**
     * Get a database object based on an id only.  Used by recovery, cleaning
     * and other clients who have an id in hand, and don't have a resident
     * node, to find the matching database for a given log entry.
     */
    public DatabaseImpl getDb(DatabaseId dbId)
        throws DatabaseException {

        return getDb(dbId, -1);
    }

    /**
     * Get a database object based on an id only. Specify the lock timeout to
     * use, or -1 to use the default timeout.  A timeout should normally only
     * be specified by daemons with their own timeout configuration.
     */
    public DatabaseImpl getDb(DatabaseId dbId, long lockTimeout)
        throws DatabaseException {

        BasicLocker locker = null;

        try {
            locker = new BasicLocker(envImpl);
            if (lockTimeout >= 0) {
                locker.setLockTimeout(lockTimeout);
            }
            return getDb(locker, dbId);
        } finally {
            if (locker != null) {
                locker.operationEnd();
            }
        }
    }
     
    /**
     * Does the real work of finding the database object, by looking in the
     * id->db tree.
     */
    private DatabaseImpl getDb(Locker locker, DatabaseId dbId)
        throws DatabaseException {

        if (dbId.equals(idDatabase.getId())) {
            /* We're looking for the id database itself. */
            return idDatabase;
        } else if (dbId.equals(nameDatabase.getId())) {
            /* We're looking for the name database itself. */
            return nameDatabase;
        } else {
            /* Scan the tree for this db. */
            CursorImpl idCursor = null;
            try {
                idCursor = new CursorImpl(idDatabase, locker);
                DatabaseEntry keyDbt = new DatabaseEntry(dbId.getBytes());
                boolean found =
		    (idCursor.searchAndPosition
		     (keyDbt, new DatabaseEntry(), SearchMode.SET, null) &
		     CursorImpl.FOUND) != 0;
                if (found) {
                    MapLN mapLN = (MapLN)
                        idCursor.getCurrentLNAlreadyLatched(LockMode.DEFAULT);
                    return mapLN.getDatabase();
                } else {
                    return null;
                }
            } finally {
                idCursor.releaseBIN();
                idCursor.close();
            }
        }
    }

    /**
     * Rebuild the IN list after recovery.
     */
    public void rebuildINListMapDb()
        throws DatabaseException {

        idDatabase.getTree().rebuildINList();
    }

    /*
     * Verification, must be run while system is quiescent.
     */
    public boolean verify(VerifyConfig config, PrintStream out)
        throws DatabaseException {

	boolean ret = true;
	try {
	    /* For now, verify all databases. */
	    idDatabase.verify(config);
	    nameDatabase.verify(config);
	} catch (DatabaseException DE) {
	    ret = false;
	}

	synchronized (envImpl.getINCompressor()) {

	    /*
	     * Get a cursor on the id tree. Use objects at the dbi layer rather
	     * than at the public api, in order to retrieve objects rather than
	     * Dbts. Note that we don't do cursor cloning here, so any failures
	     * from each db verify invalidate the cursor.
	     */
	    Locker locker = null;
	    CursorImpl cursor = null;
	    try {
		locker = new BasicLocker(envImpl);
		cursor = new CursorImpl(idDatabase, locker);
		cursor.positionFirstOrLast(true, null);
		MapLN mapLN = (MapLN) cursor.
		    getCurrentLNAlreadyLatched(LockMode.DEFAULT);

		DatabaseEntry keyDbt = new DatabaseEntry();
		DatabaseEntry dataDbt = new DatabaseEntry();

		while (mapLN != null) {
		    mapLN.getDatabase().verify(config);
		    /* Go on to the next entry. */
		    OperationStatus status =
			cursor.getNext(keyDbt, dataDbt, LockMode.DEFAULT,
				       true,   // go forward
				       false); // do need to latch
		    if (status != OperationStatus.SUCCESS) {
			break;
		    }
		    mapLN = (MapLN) cursor.getCurrentLN();
		}
	    } catch (DatabaseException e) {
		out.println("Verification error" + e.getMessage());
		ret = false;
	    } finally {
		if (cursor != null) {
		    cursor.releaseBINs();
		    cursor.close();
		}
		if (locker != null) {
		    locker.operationEnd();
		}
	    }
	}

	return ret;
    }

    /**
     * Return the database name for a given db. Slow, must traverse. Used by
     * truncate and for debugging.
     */
    public String getDbName(DatabaseId id) 
        throws DatabaseException { 

        if (id.equals(ID_DB_ID)) {
            return ID_DB_NAME;
        } else if (id.equals(NAME_DB_ID)) {
            return NAME_DB_NAME;
        }

        Locker locker = null;
        CursorImpl cursor = null;
        try {
            locker = new BasicLocker(envImpl);
            CursorConfig cursorConfig = new CursorConfig();
            cursorConfig.setDirtyRead(true);
            cursor = new CursorImpl(nameDatabase, locker, 
				    true, cursorConfig); // use dirty reads
            DatabaseEntry keyDbt = new DatabaseEntry();
            DatabaseEntry dataDbt = new DatabaseEntry();
            String name = null;
            if (cursor.positionFirstOrLast(true, null)) {
                /* Fill in the key DatabaseEntry */
                cursor.getCurrentAlreadyLatched(keyDbt, dataDbt,
                                                LockMode.DEFAULT, true);
                OperationStatus status;
                do {
                    NameLN nameLN =
                        (NameLN) cursor.getCurrentLN();
                    if (nameLN.getId().equals(id)) {
                        name = new String(keyDbt.getData(), "UTF-8");
                        break;
                    }

                    /* Go on to the next entry. */
                    status = cursor.getNext(keyDbt, dataDbt, LockMode.DEFAULT,
                                            true,   // go forward
                                            false); // do need to latch
                } while (status == OperationStatus.SUCCESS);
            }
            return name;
	} catch (UnsupportedEncodingException UEE) {
	    throw new DatabaseException(UEE);
        } finally {
            if (cursor != null) {
                cursor.releaseBINs();
                cursor.close();
            }
            if (locker != null) {
                locker.operationEnd();
            }
        }
    }
    
    /**
     * @return a list of database names held in the environment, as strings.
     */
    public List getDbNames()
        throws DatabaseException {
        
        List nameList = new ArrayList();
        Locker locker = null;
        CursorImpl cursor = null;
        try {
            locker = new BasicLocker(envImpl);
            cursor = new CursorImpl(nameDatabase, locker);
            DatabaseEntry keyDbt = new DatabaseEntry();
            DatabaseEntry dataDbt = new DatabaseEntry();
            if (cursor.positionFirstOrLast(true, null)) {
                OperationStatus status;
                cursor.getCurrentAlreadyLatched(keyDbt, dataDbt,
                                                LockMode.DEFAULT, true);
                do {
                    String name = new String(keyDbt.getData(), "UTF-8");
                    if (!isReservedDbName(name)) {
                        nameList.add(name);
                    }

                    /* Go on to the next entry. */
                    status = cursor.getNext(keyDbt, dataDbt, LockMode.DEFAULT,
                                            true,   // go forward
                                            false); // do need to latch
                } while (status == OperationStatus.SUCCESS);
            }
            return nameList;
	} catch (UnsupportedEncodingException UEE) {
	    throw new DatabaseException(UEE);
        } finally {
            if (cursor != null) {
                cursor.close();
            }

            if (locker != null) {
                locker.operationEnd();
            }
        }
    }

    /**
     * Returns true if the name is a reserved JE database name.
     */
    public boolean isReservedDbName(String name) {
        for (int i = 0; i < RESERVED_DB_NAMES.length; i += 1) {
            if (RESERVED_DB_NAMES[i].equals(name)) {
                return true;
            }
        }
        return false;
    }

    /**
     * @return the higest level node in the environment.
     */
    public int getHighestLevel()
        throws DatabaseException {

        /* The highest level in the map side */
        RootLevel getLevel = new RootLevel(idDatabase);
        idDatabase.getTree().withRootLatched(getLevel);
        int idHighLevel = getLevel.getRootLevel();

        /* The highest level in the name side */
        getLevel = new RootLevel(nameDatabase);
        nameDatabase.getTree().withRootLatched(getLevel);
        int nameHighLevel = getLevel.getRootLevel();

        return (nameHighLevel>idHighLevel)? nameHighLevel : idHighLevel;
    }

    /*
     * RootLevel lets us write out the root IN within the root latch.
     */
    private static class RootLevel implements WithRootLatched {
        private DatabaseImpl db;
        private int rootLevel;


        RootLevel(DatabaseImpl db) {
            this.db = db;
            rootLevel = 0;
        }

        /**
         * @return true if the in-memory root was replaced.
         */
        public IN doWork(ChildReference root) 
            throws DatabaseException {
            
            IN rootIN = (IN) root.fetchTarget(db, null);
            rootLevel = rootIN.getLevel();
            return null;
        }

        int getRootLevel() {
            return rootLevel;
        }
    }

    /*
     * LoggableObject
     */

    /**
     * @see LoggableObject#getLogType
     */
    public LogEntryType getLogType() {
        return LogEntryType.LOG_ROOT;
    }

    /**
     * @see LoggableObject#marshallOutsideWriteLatch
     * Can be marshalled outside the log write latch.
     */
    public boolean marshallOutsideWriteLatch() {
        return true;
    }

    /**
     * @see LoggableObject#getLogSize
     */
    public int getLogSize() {
        return
            LogUtils.getIntLogSize() +        // last allocated id
            idDatabase.getLogSize() +         // id db
            nameDatabase.getLogSize();        // name db
    }

    /**
     * @see LoggableObject#writeToLog
     */
    public void writeToLog(ByteBuffer logBuffer) {
        LogUtils.writeInt(logBuffer,lastAllocatedDbId);  // last id
        idDatabase.writeToLog(logBuffer);                // id db
        nameDatabase.writeToLog(logBuffer);              // name db
    }

    /**
     * @see LoggableObject#postLogWork
     */
    public void postLogWork(DbLsn justLoggedLsn) 
        throws DatabaseException {
    }

    /*
     * LogReadable
     */

    /**
     * @see LogReadable#readFromLog
     */
    public void readFromLog(ByteBuffer itemBuffer)
        throws LogException {
        lastAllocatedDbId = LogUtils.readInt(itemBuffer); // last id
        idDatabase.readFromLog(itemBuffer);               // id db
        nameDatabase.readFromLog(itemBuffer);             // name db
    }
    
    /**
     * @see LogReadable#dumpLog
     */
    public void dumpLog(StringBuffer sb, boolean verbose) {
        sb.append("<dbtree lastId = \"");
        sb.append(lastAllocatedDbId);
        sb.append("\">");
        sb.append("<idDb>");
        idDatabase.dumpLog(sb, verbose);
        sb.append("</idDb><nameDb>");
        nameDatabase.dumpLog(sb, verbose);
        sb.append("</nameDb>");
        sb.append("</dbtree>");
    }

    /**
     * @see LogReadable#logEntryIsTransactional.
     */
    public boolean logEntryIsTransactional() {
	return false;
    }

    /**
     * @see LogReadable#getTransactionId
     */
    public long getTransactionId() {
	return 0;
    }

    /*
     * For unit test support
     */

    String dumpString(int nSpaces) {
        StringBuffer self = new StringBuffer();
        self.append(TreeUtils.indent(nSpaces));
        self.append("<dbTree lastDbId =\"");
        self.append(lastAllocatedDbId);
        self.append("\">");
        self.append('\n');
        self.append(idDatabase.dumpString(nSpaces + 1));
        self.append('\n');
        self.append(nameDatabase.dumpString(nSpaces + 1));
        self.append('\n');
        self.append("</dbtree>");
        return self.toString();
    }   
        
    public String toString() {
        return dumpString(0);
    }

    /**
     * For debugging.
     */
    public void dump()
        throws DatabaseException {

        idDatabase.getTree().dump();
        nameDatabase.getTree().dump();
    }
}
