/*
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002-2004
 *      Sleepycat Software.  All rights reserved.
 *
 * $Id: DeltaInfo.java,v 1.1 2004/11/22 18:27:57 kate Exp $
 */
package com.sleepycat.je.tree;

import java.nio.ByteBuffer;

import com.sleepycat.je.log.LogException;
import com.sleepycat.je.log.LogReadable;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.log.LogWritable;
import com.sleepycat.je.utilint.DbLsn;

/**
 * DeltaInfo holds the delta for one BIN entry in a partial BIN log entry.
 * The data here is all that we need to update a BIN to its proper state.
 */

public class DeltaInfo implements LogWritable, LogReadable {
    private Key key;
    private DbLsn lsn;
    private boolean knownDeleted;
		  
    DeltaInfo(Key key, DbLsn lsn, boolean knownDeleted) {
        this.key = key;
        this.lsn = lsn;
        this.knownDeleted = knownDeleted;
    }

    /**
     * For reading from the log only.
     */
    DeltaInfo() {
        key = new Key();
        lsn = new DbLsn();
    }

    /* 
     * @see com.sleepycat.je.log.LogWritable#getLogSize()
     */
    public int getLogSize() {
        return key.getLogSize() +
            lsn.getLogSize() +
            LogUtils.getBooleanLogSize();
    }

    /* 
     * @see LogWritable#writeToLog(java.nio.ByteBuffer)
     */
    public void writeToLog(ByteBuffer logBuffer) {
        key.writeToLog(logBuffer);
        lsn.writeToLog(logBuffer);
        LogUtils.writeBoolean(logBuffer,knownDeleted);
    }

    /* 
     * @see com.sleepycat.je.log.LogReadable#readFromLog
     *          (java.nio.ByteBuffer)
     */
    public void readFromLog(ByteBuffer itemBuffer) throws LogException {
        key.readFromLog(itemBuffer);
        lsn.readFromLog(itemBuffer);
        knownDeleted = LogUtils.readBoolean(itemBuffer);
    }

    /* 
     * @see LogReadable#dumpLog(java.lang.StringBuffer)
     */
    public void dumpLog(StringBuffer sb, boolean verbose) {
        key.dumpLog(sb, verbose);
        lsn.dumpLog(sb, verbose);
        sb.append("<knownDeleted val=\"").append(knownDeleted).append("\"/>");
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

    /**
     * @return the Key.
     */
    Key getKey() {
        return key;
    }

    /**
     * @return true if this is known to be deleted.
     */
    boolean isKnownDeleted() {
        return knownDeleted;
    }

    /**
     * @return the LSN.
     */
    DbLsn getLsn() {
        return lsn;
    }

}
