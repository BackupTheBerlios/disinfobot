/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: LNFileReader.java,v 1.1 2004/11/22 18:27:58 kate Exp $
*/

package com.sleepycat.je.log;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.entry.LNLogEntry;
import com.sleepycat.je.log.entry.LogEntry;
import com.sleepycat.je.tree.Key;
import com.sleepycat.je.tree.LN;
import com.sleepycat.je.txn.TxnCommit;
import com.sleepycat.je.utilint.DbLsn;

/**
 * LNFileReader scans log files for LNs. Also, if it's going backwards for
 * the undo phase in recovery, it reads transaction commit entries.
 */
public class LNFileReader extends FileReader {
    /* 
     * targetEntryMap maps DbLogEntryTypes to log entries. We use this
     * collection to find the right LogEntry instance to read in the
     * current entry.
     */
    protected Map targetEntryMap;
    protected LogEntry targetLogEntry;
        
    /**
     * Create this reader to start at a given LSN.
     * @param env The relevant EnvironmentImpl
     * @param readBufferSize buffer size in bytes for reading in log
     * @param startLsn where to start in the log
     * @param redo If false, we're going to go forward from
     *             the start lsn to the end of the log. If true, we're going
     *             backwards from the end of the log to the start lsn. 
     * @param endOfFileLsn the virtual lsn that marks the end of the log. (The
     *  one off the end of the log). Only used if we're reading backwards.
     *  Different from the startLsn because the startLsn tells us where the
     *  beginning of the start entry is, but not the length/end of the start
     *  entry. May be null if we're going foward.
     * @param finishLsn the last lsn to read in the log. May be null if we
     *  want to read to the end of the log.
     */
    public LNFileReader(EnvironmentImpl env,
                        int readBufferSize, 
                        DbLsn startLsn,
                        boolean redo,
                        DbLsn endOfFileLsn,
                        DbLsn finishLsn,
			Long singleFileNum)
        throws IOException, DatabaseException {

        super(env, readBufferSize, redo, startLsn,
              singleFileNum, endOfFileLsn, finishLsn);

        targetEntryMap = new HashMap();
    }

    public void addTargetType(LogEntryType entryType)
        throws DatabaseException {

        targetEntryMap.put(entryType, entryType.getNewLogEntry());
    }

    /** 
     * @return true if this is a transactional LN or Locker Commit entry.
     */
    protected boolean isTargetEntry(byte entryTypeNum,
                                    byte entryTypeVersion) {

        if (LogEntryType.isProvisional(entryTypeVersion)) {
            /* Skip provisionial entries */
            targetLogEntry = null;
        } else {
            LogEntryType fromLogType =
                new LogEntryType(entryTypeNum, entryTypeVersion);
                                                            
            /* Is it a target entry? */
            targetLogEntry = (LogEntry) targetEntryMap.get(fromLogType);
        }
        return (targetLogEntry != null);
    }

    /**
     * This reader instantiates an LN and key for every LN entry.
     */
    protected boolean processEntry(ByteBuffer entryBuffer)
        throws DatabaseException {

        targetLogEntry.readEntry(entryBuffer);
        return true;
    }

    /**
     * @return true if the last entry was an LN.
     */
    public boolean isLN() {
        return (targetLogEntry instanceof LNLogEntry);
    }

    /**
     * Get the last LN seen by the reader.
     */
    public LN getLN() {
        return ((LNLogEntry) targetLogEntry).getLN();
    }

    /**
     * Get the last databaseId seen by the reader.
     */
    public DatabaseId getDatabaseId() {
        return ((LNLogEntry) targetLogEntry).getDbId();
    }

    /**
     * Get the last key seen by the reader.
     */
    public Key getKey() {
        return ((LNLogEntry) targetLogEntry).getKey();
    }

    /**
     * Get the last key seen by the reader.
     */
    public Key getDupTreeKey() {
        return ((LNLogEntry) targetLogEntry).getDupKey();
    }

    /**
     * @return the transaction id of the current entry.
     */
    public Long getTxnId() {
        return ((LNLogEntry) targetLogEntry).getTxnId();
    }

    /**
     * Get the last txn commit id seen by the reader.
     */
    public long getTxnCommitId() {
        return ((TxnCommit) targetLogEntry.getMainItem()).getId();
    }

    /**
     * Get node id of current LN.
     */
    public long getNodeId() {
        return ((LNLogEntry) targetLogEntry).getLN().getNodeId();
    }

    /**
     * Get last abort lsn seen by the reader (may be null).
     */
    public DbLsn getAbortLsn() {
        return ((LNLogEntry) targetLogEntry).getAbortLsn();
    }

    /**
     * Get last abort known deleted seen by the reader.
     */
    public boolean getAbortKnownDeleted() {
        return ((LNLogEntry) targetLogEntry).getAbortKnownDeleted();
    }
}
