/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: INDeleteInfo.java,v 1.1 2004/11/22 18:27:57 kate Exp $
*/

package com.sleepycat.je.tree;

import java.nio.ByteBuffer;

import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogException;
import com.sleepycat.je.log.LogReadable;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.log.LogWritable;
import com.sleepycat.je.log.LoggableObject;
import com.sleepycat.je.utilint.DbLsn;

/**
 * INDeleteInfo encapsulates the information logged about the removal of
 * a child from an IN during IN compression.
 */
public class INDeleteInfo
    implements LoggableObject, LogReadable, LogWritable {

    private long deletedNodeId;
    private Key deletedIdKey;
    private DatabaseId dbId;

    /**
     * Create a new delete info entry.
     */
    public INDeleteInfo(long deletedNodeId, Key deletedIdKey, DatabaseId dbId){
        this.deletedNodeId = deletedNodeId;
        this.deletedIdKey = deletedIdKey;
        this.dbId = dbId;
    }

    /**
     * Used by logging system only.
     */
    public INDeleteInfo() {
        deletedIdKey = new Key();
        dbId = new DatabaseId();
    }

    /*
     * Accessors.
     */
    public long getDeletedNodeId() {
        return deletedNodeId;
    }

    public Key getDeletedIdKey(){
        return deletedIdKey;
    }
    
    public DatabaseId getDatabaseId() {
        return dbId;
    }

    /*
     * Logging support for writing.
     */

    /**
     * @see LoggableObject#getLogType
     */
    public LogEntryType getLogType() {
        return LogEntryType.LOG_IN_DELETE_INFO;
    }

    /**
     * @see LoggableObject#marshallOutsideWriteLatch
     * Can be marshalled outside the log write latch.
     */
    public boolean marshallOutsideWriteLatch() {
        return true;
    }

    /**
     * @see LoggableObject#postLogWork
     */
    public void postLogWork(DbLsn justLoggedLsn) {
    }

    /**
     * @see LoggableObject#getLogSize
     */
    public int getLogSize() {
        return LogUtils.LONG_BYTES +
            deletedIdKey.getLogSize() +
            dbId.getLogSize();
    }

    /**
     * @see LogWritable#writeToLog
     */
    public void writeToLog(ByteBuffer logBuffer) {

        LogUtils.writeLong(logBuffer, deletedNodeId);
        deletedIdKey.writeToLog(logBuffer);
        dbId.writeToLog(logBuffer);
    }

    /**
     * @see LogReadable#readFromLog
     */
    public void readFromLog(ByteBuffer itemBuffer)
	throws LogException {

        deletedNodeId = LogUtils.readLong(itemBuffer);
        deletedIdKey.readFromLog(itemBuffer);
        dbId.readFromLog(itemBuffer);
    }

    /**
     * @see LogReadable#dumpLog
     */
    public void dumpLog(StringBuffer sb, boolean verbose) {
        sb.append("<INDeleteEntry node=\"").append(deletedNodeId);
        sb.append("\">");
        deletedIdKey.dumpLog(sb, verbose);
        dbId.dumpLog(sb, verbose);
        sb.append("</INDeleteEntry>");
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
