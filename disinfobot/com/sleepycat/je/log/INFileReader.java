/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: INFileReader.java,v 1.1 2004/11/22 18:27:58 kate Exp $
*/

package com.sleepycat.je.log;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DbTree;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.entry.INContainingEntry;
import com.sleepycat.je.log.entry.LNLogEntry;
import com.sleepycat.je.log.entry.LogEntry;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.tree.INDeleteInfo;
import com.sleepycat.je.tree.Key;
import com.sleepycat.je.tree.MapLN;
import com.sleepycat.je.tree.Node;
import com.sleepycat.je.utilint.DbLsn;

/**
 * INFileReader supports recovery by scanning log files during the IN rebuild
 * pass. It looks for internal nodes (all types), segregated by whether they
 * belong to the main tree or the duplicate trees. This file reader can also 
 * be configured to keep track of the maximum node id, database id and 
 * txn id seen so those sequences can be updated properly at recovery.
 */
public class INFileReader extends FileReader {
    /* information about the last entry seen */
    private boolean lastEntryWasDelete;

    /* 
     * targetEntryMap maps DbLogEntryTypes to log entries. We use this
     * collection to find the right LogEntry instance to read in the
     * current entry.
     */
    private Map targetEntryMap;
    private LogEntry targetLogEntry;

    private Map dbIdTrackingMap;
    private LNLogEntry dbIdTrackingEntry;

    private Map txnIdTrackingMap;
    private LNLogEntry txnIdTrackingEntry;

    /*
     * If trackIds is true, peruse all node entries for the maximum
     * node id, check all MapLNs for the maximum db id, and check all
     * LNs for the maximum txn id
     */
    private boolean trackIds;
    private long maxNodeId;
    private int maxDbId;
    private long maxTxnId;
    private boolean mapDbOnly;

    /**
     * Create this reader to start at a given LSN.
     */
    public INFileReader(EnvironmentImpl env,
                        int readBufferSize, 
                        DbLsn startLsn,
                        boolean trackIds,
			boolean mapDbOnly)
        throws IOException, DatabaseException {
        super(env, readBufferSize, true, startLsn,
              null, null, null);

        this.trackIds = trackIds;
        maxNodeId = 0;
        maxDbId = 0;
	this.mapDbOnly = mapDbOnly;

        targetEntryMap = new HashMap();
        dbIdTrackingMap = new HashMap();
        txnIdTrackingMap = new HashMap();

        if (trackIds) {
            dbIdTrackingMap.put(LogEntryType.LOG_MAPLN_TRANSACTIONAL,
                                LogEntryType.LOG_MAPLN_TRANSACTIONAL.
				getNewLogEntry());
            dbIdTrackingMap.put(LogEntryType.LOG_MAPLN,
                                LogEntryType.LOG_MAPLN.getNewLogEntry());
            txnIdTrackingMap.put(LogEntryType.LOG_LN_TRANSACTIONAL,
                                 LogEntryType.LOG_LN_TRANSACTIONAL.
				 getNewLogEntry());
            txnIdTrackingMap.put(LogEntryType.LOG_MAPLN_TRANSACTIONAL,
                                 LogEntryType.LOG_MAPLN_TRANSACTIONAL.
				 getNewLogEntry());
            txnIdTrackingMap.put(LogEntryType.LOG_NAMELN_TRANSACTIONAL,
                                 LogEntryType.LOG_NAMELN_TRANSACTIONAL.
				 getNewLogEntry());
        }
    }

    /**
     * Configure this reader to target this kind of entry.
     */
    public void addTargetType(LogEntryType entryType)
        throws DatabaseException {

        targetEntryMap.put(entryType, entryType.getNewLogEntry());
    }


    /** 
     * If we're tracking node, database and txn ids, we want to see all node
     * log entries. If not, we only want to see IN entries.
     * @return true if this is an IN entry.
     */
    protected boolean isTargetEntry(byte entryTypeNum,
                                    byte entryTypeVersion) {
        lastEntryWasDelete = false;
        targetLogEntry = null;
        dbIdTrackingEntry = null;
        txnIdTrackingEntry = null;

        /*
         * Get the log entry type instance we need to read the entry.
         * If the entry is provisional, we won't be reading it in its
         * entirety, so we don't need to establish targetLogEntry.
         */
        if (!LogEntryType.isProvisional(entryTypeVersion)) {
            LogEntryType fromLogType =
		new LogEntryType(entryTypeNum, entryTypeVersion);
                                                            
            /* Is it a target entry? */
            targetLogEntry = (LogEntry) targetEntryMap.get(fromLogType);

            if (targetLogEntry == null) {

                /*
                 * This isn't the main type of log entry that needs to 
                 * be processed. Check if it's an id tracking entry.
                 */
                dbIdTrackingEntry = (LNLogEntry)
                    dbIdTrackingMap.get(fromLogType);
                txnIdTrackingEntry = (LNLogEntry)
                    txnIdTrackingMap.get(fromLogType);
            } else {
                if (LogEntryType.LOG_IN_DELETE_INFO.equals(fromLogType)) {
                    lastEntryWasDelete = true;
                }
            }
        }

        /*
         * Return true if this entry should be passed on to processEntry.
         * If we're tracking ids, return if this is a targeted entry
         * if if it's any kind of node. If we're not tracking ids,
         * only return true if it's a targeted entry.
         */
        if (trackIds) {
            return (targetLogEntry != null) ||
                (dbIdTrackingEntry != null) ||
                LogEntryType.isNodeType(entryTypeNum, entryTypeVersion);
        } else {
            return (targetLogEntry != null);
        }
    }

    /**
     * This reader looks at all nodes for the max node id and database id. It
     * only returns non-provisional INs and IN delete entries.
     */
    protected boolean processEntry(ByteBuffer entryBuffer)
        throws DatabaseException {

        long entryNodeId = 0;
        boolean nodeIdSet = false;
        boolean useEntry = false;

        /* 
         * If this is a targetted entry, read the entire log entry. Otherwise,
         * we only want to pick out the node id.
         */
        if (targetLogEntry != null) {
            targetLogEntry.readEntry(entryBuffer);
	    DatabaseId dbId = getDatabaseId();
	    boolean isMapDb = dbId.equals(DbTree.ID_DB_ID);
	    
	    if (mapDbOnly && !isMapDb) {
		useEntry = false;
	    } else {
		if (!lastEntryWasDelete) {
		    entryNodeId = ((INContainingEntry) targetLogEntry).
			getIN(env).getNodeId();
		}
		useEntry = true;
	    }
        } else if (trackIds) {

            /*
             * This is not an IN we need to process, but we still need to
             * check the db id, txn id and node id
             */
            /* This entry has a db id */
            LNLogEntry lnEntry = null;
            if (dbIdTrackingEntry != null) {
                lnEntry = dbIdTrackingEntry;
                lnEntry.readEntry(entryBuffer);
                MapLN mapLN = (MapLN) lnEntry.getMainItem();
                int dbId = mapLN.getDatabase().getId().getId();
                if (dbId > maxDbId) {
                    maxDbId = dbId;
                }
                entryNodeId = lnEntry.getLN().getNodeId();
                nodeIdSet = true;
            }

            /* This entry has a txn id */
            if (txnIdTrackingEntry != null) {
                if (lnEntry == null) {
                    /* entry hasn't been read yet. */
                    lnEntry = txnIdTrackingEntry;
                    lnEntry.readEntry(entryBuffer);
                }
                long txnId = lnEntry.getTxnId().longValue();
                if (txnId > maxTxnId) {
                    maxTxnId = txnId;
                }
                entryNodeId = lnEntry.getLN().getNodeId();
                nodeIdSet = true;
            }

            if (!nodeIdSet) {
                /* Not a targetted node, just read the node id. */
                entryNodeId = Node.readNodeIdFromLog(entryBuffer, 
                                                     currentEntrySize);
            }
        }

        /* Keep track of the largest node id seen. */
        if (trackIds) {
            maxNodeId = (entryNodeId > maxNodeId) ? entryNodeId: maxNodeId;
        }

        /* Return true if this entry should be processed */
        return useEntry;
    }

    /**
     * Get the last IN seen by the reader.
     */
    public IN getIN() 
    	throws DatabaseException {
    		
        return ((INContainingEntry) targetLogEntry).getIN(env);
    }

    /**
     * Get the last databaseId seen by the reader.
     */
    public DatabaseId getDatabaseId() {
        if (lastEntryWasDelete) {
            return ((INDeleteInfo) targetLogEntry.getMainItem()).
                getDatabaseId();
        } else {
            return ((INContainingEntry) targetLogEntry).getDbId();
        }
    }

    /**
     * Get the maximum node id seen by the reader
     */
    public long getMaxNodeId() {
        return maxNodeId;
    }

    /**
     * Get the maximum db id seen by the reader
     */
    public int getMaxDbId() {
        return maxDbId;
    }

    /**
     * Get the maximum txn id seen by the reader
     */
    public long getMaxTxnId() {
        return maxTxnId;
    }

    /**
     * @return true if the last entry was a delete info entry.
     */
    public boolean isDeleteInfo() {
        return lastEntryWasDelete;
    }
    
    /**
     * Get the deleted node id stored in the last delete info log entry
     */
    public long getDeletedNodeId() {
        return ((INDeleteInfo)
                targetLogEntry.getMainItem()).getDeletedNodeId();
    }

    /**
     * Get the deleted id key stored in the last delete info log entry
     */
    public Key getDeletedIdKey() {
        return ((INDeleteInfo)
                targetLogEntry.getMainItem()).getDeletedIdKey();
    }

    /**
     * Get the LSN that should represent this IN. For most INs, it's the LSN
     * that was just read. For BINDelta entries, it's the lsn of the last
     * full version.
     */
    public DbLsn getLsnOfIN() {
        return ((INContainingEntry) targetLogEntry).getLsnOfIN(getLastLsn());
    }
}
