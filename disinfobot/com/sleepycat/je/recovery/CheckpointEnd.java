/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: CheckpointEnd.java,v 1.1 2004/11/22 18:27:59 kate Exp $
*/

package com.sleepycat.je.recovery;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.Calendar;

import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogException;
import com.sleepycat.je.log.LogReadable;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.log.LoggableObject;
import com.sleepycat.je.utilint.DbLsn;

/**
 * CheckpointEnd encapsulates the information needed by a checkpoint end 
 * log entry.
 */
public class CheckpointEnd implements LoggableObject, LogReadable {
    
    /* 
     * invoker is just a way to tag each checkpoint in the
     * log for easier log based debugging. It will tell us whether the
     * checkpoint was invoked by recovery, the daemon, the api, or
     * the cleaner.
     */
    private String invoker; 

    private Timestamp endTime;
    private DbLsn checkpointStartLsn;
    private boolean rootLsnExists;
    private DbLsn rootLsn;
    private DbLsn firstActiveLsn;
    private long lastNodeId;
    private int lastDbId;
    private long lastTxnId;
    private long id;

    public CheckpointEnd(String invoker,
                         DbLsn checkpointStartLsn,
                         DbLsn rootLsn,
                         DbLsn firstActiveLsn,
                         long lastNodeId,
                         int lastDbId,
                         long lastTxnId,
                         long id) {
        if (invoker == null) {
            this.invoker = "";
        } else {
            this.invoker = invoker;
        }
            
        Calendar cal = Calendar.getInstance();
        this.endTime = new Timestamp(cal.getTime().getTime());
        this.checkpointStartLsn = checkpointStartLsn;
        this.rootLsn = rootLsn;
        if (rootLsn == null) {
            rootLsnExists = false;
        } else {
            rootLsnExists = true;
        }
        if (firstActiveLsn == null) {
            this.firstActiveLsn = checkpointStartLsn;
        } else {
            this.firstActiveLsn = firstActiveLsn;
        }
        this.lastNodeId = lastNodeId;
        this.lastDbId = lastDbId;
        this.lastTxnId = lastTxnId;
        this.id = id;
    }

    /* For logging only */
    public CheckpointEnd() {
        checkpointStartLsn = new DbLsn();
        rootLsn = new DbLsn();
        firstActiveLsn = new DbLsn();
    }

    /*
     * Logging support for writing to the log
     */

    /**
     * @see LoggableObject#getLogType
     */
    public LogEntryType getLogType() {
        return LogEntryType.LOG_CKPT_END;
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
        int size =
            LogUtils.getStringLogSize(invoker) + // invoker
            LogUtils.getTimestampLogSize() +     // endTime
            checkpointStartLsn.getLogSize() +  
            LogUtils.getBooleanLogSize() +       // rootLsnExists
            firstActiveLsn.getLogSize() +
            LogUtils.getLongLogSize() +          // lastNodeId
            LogUtils.getIntLogSize() +           // lastDbId
            LogUtils.getLongLogSize() +          // lastTxnId
            LogUtils.getLongLogSize();           // id

        if (rootLsnExists) {
            size += rootLsn.getLogSize();
        }
        return size;
    }

    /**
     * @see LoggableObject#writeToLog
     */
    public void writeToLog(ByteBuffer logBuffer) {
        LogUtils.writeString(logBuffer, invoker);
        LogUtils.writeTimestamp(logBuffer, endTime);
        checkpointStartLsn.writeToLog(logBuffer);
        LogUtils.writeBoolean(logBuffer, rootLsnExists);
        if (rootLsnExists) {
            rootLsn.writeToLog(logBuffer);
        }
        firstActiveLsn.writeToLog(logBuffer);
        LogUtils.writeLong(logBuffer, lastNodeId);
        LogUtils.writeInt(logBuffer, lastDbId);
        LogUtils.writeLong(logBuffer, lastTxnId);
        LogUtils.writeLong(logBuffer, id);
    }

    /**
     * @see LogReadable#readFromLog
     */
    public void readFromLog(ByteBuffer logBuffer)
	throws LogException {
        invoker = LogUtils.readString(logBuffer);
        endTime = LogUtils.readTimestamp(logBuffer);
        checkpointStartLsn.readFromLog(logBuffer);
        rootLsnExists = LogUtils.readBoolean(logBuffer);
        if (rootLsnExists) {
            rootLsn.readFromLog(logBuffer);
        }
        firstActiveLsn.readFromLog(logBuffer);
        lastNodeId = LogUtils.readLong(logBuffer);
        lastDbId = LogUtils.readInt(logBuffer);
        lastTxnId = LogUtils.readLong(logBuffer);
        id = LogUtils.readLong(logBuffer);
    }

    /**
     * @see LogReadable#dumpLog
     */
    public void dumpLog(StringBuffer sb, boolean verbose) {
        sb.append("<CkptEnd invoker=\"").append(invoker);
        sb.append("\" time=\"").append(endTime);
        sb.append("\" lastNodeId=\"").append(lastNodeId);
        sb.append("\" lastDbId=\"").append(lastDbId);
        sb.append("\" lastTxnId=\"").append(lastTxnId);
        sb.append("\" id=\"").append(id);
        sb.append("\" rootExists=\"").append(rootLsnExists);
        sb.append("\">");
        sb.append("<ckptStart>");
        checkpointStartLsn.dumpLog(sb, verbose);
        sb.append("</ckptStart>");

        if (rootLsnExists) {
            sb.append("<root>");;
            rootLsn.dumpLog(sb, verbose);
            sb.append("</root>");;
        }
        sb.append("<firstActive>");;
        firstActiveLsn.dumpLog(sb, verbose);
        sb.append("</firstActive>");;
        sb.append("</CkptEnd>");
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

    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("time=").append(endTime);
        sb.append(" lastNodeId=").append(lastNodeId);
        sb.append(" lastDbId=").append(lastDbId);
        sb.append(" lastTxnId=").append(lastTxnId);
        sb.append(" id=").append(id);
        sb.append(" rootExists=").append(rootLsnExists);
        sb.append(" ckptStartLsn=").append
            (checkpointStartLsn.getNoFormatString());
        if (rootLsnExists) {
            sb.append(" root=").append(rootLsn.getNoFormatString());
        }
        sb.append(" firstActive=").append(firstActiveLsn.getNoFormatString());
        return sb.toString();
    }

    /*
     * Accessors
     */
    DbLsn getCheckpointStartLsn() {
        return checkpointStartLsn;
    }

    DbLsn getRootLsn() {
        return rootLsn;
    }

    DbLsn getFirstActiveLsn() {
        return firstActiveLsn;
    }

    long getLastNodeId() {
        return lastNodeId;
    }
    int getLastDbId() {
        return lastDbId;
    }
    long getLastTxnId() {
        return lastTxnId;
    }
    long getId() {
        return id;
    }

}
