/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: TxnEnd.java,v 1.1 2004/11/22 18:27:57 kate Exp $
*/

package com.sleepycat.je.txn;

import java.nio.ByteBuffer;
import java.sql.Timestamp;

import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogReadable;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.log.LoggableObject;
import com.sleepycat.je.utilint.DbLsn;

/**
 * This class writes out a transaction commit or transaction end record.
 */
public abstract class TxnEnd
    implements LoggableObject, LogReadable {

    private long id;
    private Timestamp time;
    private DbLsn lastLsn;

    TxnEnd(long id, DbLsn lastLsn) {
        this.id = id;
        time = new Timestamp(System.currentTimeMillis());
        this.lastLsn = lastLsn;
    }
    
    /**
     * For constructing from the log
     */
    public TxnEnd() {
        lastLsn = new DbLsn();
    }

    /*
     * Accessors.
     */
    public long getId() {
        return id;
    }

    DbLsn getLastLsn() {
        return lastLsn;
    }

    protected abstract String getTagName();

    /*
     * Log support for writing.
     */

    /**
     * @see LoggableObject#getLogType
     */
    public abstract LogEntryType getLogType();

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
            LogUtils.getTimestampLogSize() +
            lastLsn.getLogSize();
    }

    /**
     * @see LoggableObject#writeToLog
     */
    public void writeToLog(ByteBuffer logBuffer) {
        LogUtils.writeLong(logBuffer, id);
        LogUtils.writeTimestamp(logBuffer,time);
        lastLsn.writeToLog(logBuffer);
    }

    /**
     * @see LogReadable#readFromLog
     */
    public void readFromLog(ByteBuffer logBuffer){
        id = LogUtils.readLong(logBuffer);
        time = LogUtils.readTimestamp(logBuffer);
        lastLsn.readFromLog(logBuffer);
    }

    /**
     * @see LogReadable#dumpLog
     */
    public void dumpLog(StringBuffer sb, boolean verbose) {
        sb.append("<").append(getTagName());
        sb.append(" id=\"").append(id);
        sb.append("\" time=\"").append(time);
        sb.append("\">");
        lastLsn.dumpLog(sb, verbose);
        sb.append("</").append(getTagName()).append(">");
    }

    /**
     * @see LogReadable#logEntryIsTransactional
     */
    public boolean logEntryIsTransactional() {
	return true;
    }

    /**
     * @see LogReadable#getTransactionId
     */
    public long getTransactionId() {
	return id;
    }
}
