/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: BINDelta.java,v 1.1 2004/11/22 18:27:57 kate Exp $
*/

package com.sleepycat.je.tree;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogException;
import com.sleepycat.je.log.LogReadable;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.log.LoggableObject;
import com.sleepycat.je.utilint.DbLsn;

/**
 * BINDelta contains the information needed to create a partial
 * (delta) BIN log entry. It also knows how to combine a full BIN log
 * entry and a delta to generate a new BIN.
 */
public class BINDelta implements LoggableObject, LogReadable {
        
    private DatabaseId dbId;    // owning db for this bin.
    private DbLsn lastFullLsn;  // location of last full version
    private List deltas;        // list of key/action changes
    private LogEntryType logEntryType; // type of log entry to use
                                         // when writing to the log.
        
    /**
     * Read a BIN and create the deltas.
     */
    public BINDelta(BIN bin) {
        lastFullLsn = bin.getLastFullVersion();
        dbId = bin.getDatabaseId();
        deltas = new ArrayList();
        logEntryType = bin.getBINDeltaType();

        /* 
         * Save every entry that has been modified since the last full version.
         * Note that we must rely on the dirty bit, and we can't infer any
         * dirtiness by comparing the last full version lsn and the
         * child reference lsn. That's because the childreference lsn may be
         * earlier than the full version lsn because of aborts.
         */
        for (int i = 0; i < bin.getNEntries(); i++) {
            ChildReference entry = bin.getEntry(i);
            if (entry.isDirty()) {
                deltas.add(new DeltaInfo(entry.getKey(),
                                         entry.getLsn(), 
                                         entry.isKnownDeleted()));
            }
        }
        
    }
        
    /**
     * For instantiating from the log.
     */
    public BINDelta() {
        dbId = new DatabaseId();
        lastFullLsn = new DbLsn();
        deltas = new ArrayList();
    }
        
    /** 
     * @return a count of deltas for this BIN.
     */
    int getNumDeltas() {
        return deltas.size();
    }
        
    /**
     * @return the dbId for this BIN.
     */
    public DatabaseId getDbId() {
        return dbId;
    }

    /**
     * @return the last full version of this BIN
     */
    public DbLsn getLastFullLsn() {
        return lastFullLsn;
    }

    /**
     * Create a BIN by starting with the full version and applying the 
     * deltas.
     */
    public BIN reconstituteBIN(EnvironmentImpl env) 
        throws DatabaseException {
                
        /* Get the last full version of this BIN */
        BIN fullBIN = (BIN) env.getLogManager().get(lastFullLsn);
        DatabaseImpl db = env.getDbMapTree().getDb(dbId);

	/* 
	 * In effect, call fullBIN.postFetchInit(db) here.  But we don't
	 * want to do that since it will put fullBIN on the INList.  Since
	 * this is either recovery or during the Cleaner run, we don't
	 * want it on the INList.
	 */
	fullBIN.setDatabase(db);
        
        /* Process each delta. */
        fullBIN.latch();
        for (int i = 0; i < deltas.size(); i++) {
            DeltaInfo info = (DeltaInfo) deltas.get(i);                

	    /*
	     * The BINDelta holds the authoritative version of each
	     * entry.  In all cases, it's entry should supercede the
	     * entry in the full BIN.  This is true even if the BIN
	     * Delta's entry is knownDeleted or if the full BIN's
	     * version is knownDeleted.  Therefore we use the flavor
	     * of findEntry that will return a knownDeleted entry if
	     * the entry key matches (i.e. true, false) but still
	     * indicates exact matches with the return index.
	     * findEntry only returns deleted entries if third arg is
	     * false, but we still need to know if it's an exact match
	     * or not so indicateExact is true.
	     */
            int foundIndex = fullBIN.findEntry(info.getKey(), true, false);
            if (foundIndex >= 0 &&
		(foundIndex & IN.EXACT_MATCH) != 0){
		foundIndex &= ~IN.EXACT_MATCH;

                /* 
                 * The entry exists in the full version, update it
                 * with the delta info.
                 */
                if (info.isKnownDeleted()) {
                    fullBIN.setKnownDeleted(foundIndex);
                } else {
		    fullBIN.clearKnownDeleted(foundIndex);
                    fullBIN.updateEntry(foundIndex, info.getLsn());
                }
            } else {
                /* The entry doesn't exist, add a new entry from the delta. */
		if (!info.isKnownDeleted()) {
		    ChildReference entry =
			new ChildReference(info.getKey(),
					   info.getLsn(),
					   info.isKnownDeleted());
		    boolean insertOk = fullBIN.insertEntry(entry);
		    assert insertOk;
		}
            }
        }

        /* 
         * Reset the generation to 0, all this manipulation might have
         * driven it up.
         */
        fullBIN.setGeneration(0);
        fullBIN.releaseLatch();
        return fullBIN;
    }
        
    /*
     * Logging support
     */

    /*
     * @see com.sleepycat.je.log.LoggableObject#getLogType()
     */
    public LogEntryType getLogType() {
        return logEntryType;
    }

    /**
     * @see LoggableObject#marshallOutsideWriteLatch
     * Can be marshalled outside the log write latch.
     */
    public boolean marshallOutsideWriteLatch() {
        return true;
    }

    /* 
     * Nothing to do after the act of logging this entry.
     * @see com.sleepycat.je.log.LoggableObject#postLogWork(
     * com.sleepycat.je.util.DbLsn)
     */
    public void postLogWork(DbLsn justLoggedLsn) {
    }

    /* 
     * @see com.sleepycat.je.log.LogReadable#readFromLog(
     * java.nio.ByteBuffer)
     */
    public void readFromLog(ByteBuffer itemBuffer) throws LogException {
        dbId.readFromLog(itemBuffer);            // database id
        lastFullLsn.readFromLog(itemBuffer);     // last version
        int numDeltas = LogUtils.readInt(itemBuffer);

        for (int i=0; i < numDeltas; i++) {      // deltas
            DeltaInfo info = new DeltaInfo();
            info.readFromLog(itemBuffer);
            deltas.add(info);
        }
    }

    /* 
     * @see com.sleepycat.je.log.LoggableObject#getLogSize()
     */
    public int getLogSize() {
        int size =
            dbId.getLogSize() +                 // database id
            lastFullLsn.getLogSize() +          // last version
            LogUtils.INT_BYTES;               // num deltas

        for (int i = 0; i < deltas.size(); i++) {    // deltas
            DeltaInfo info = (DeltaInfo) deltas.get(i);
            size += info.getLogSize();
        }      
        
        return size;  
    }

    /* 
     * @see com.sleepycat.je.log.LoggableObject#writeToLog
     *             (java.nio.ByteBuffer)
     */
    public void writeToLog(ByteBuffer logBuffer) {
        dbId.writeToLog(logBuffer);                     // database id
        lastFullLsn.writeToLog(logBuffer);              // last version
        LogUtils.writeInt(logBuffer, deltas.size());  // num deltas
        
        for (int i = 0; i < deltas.size(); i++) {              // deltas
            DeltaInfo info = (DeltaInfo) deltas.get(i);
            info.writeToLog(logBuffer);
        }        
    }

    /* 
     * @see LogReadable#dumpLog(java.lang.StringBuffer)
     */
    public void dumpLog(StringBuffer sb, boolean verbose) {
        dbId.dumpLog(sb, verbose);
        sb.append("<lastFullLsn>");
        lastFullLsn.dumpLog(sb, verbose);
        sb.append("</lastFullLsn>");
        sb.append("<deltas size=\"").append(deltas.size()).append("\"/>");
        for (int i = 0; i < deltas.size(); i++) {    // deltas
            DeltaInfo info = (DeltaInfo) deltas.get(i);
            info.dumpLog(sb, verbose);
        }      
    }

    /**
     * @see LogReadable#logEntryIsTransactional
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
}
