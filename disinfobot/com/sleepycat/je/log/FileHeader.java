/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: FileHeader.java,v 1.1 2004/11/22 18:27:58 kate Exp $
*/

package com.sleepycat.je.log;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.Calendar;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.utilint.DbLsn;

/**
 * A FileHeader embodies the header information at the beginning of each log
 * file.
 *
 * @version $Id: FileHeader.java,v 1.1 2004/11/22 18:27:58 kate Exp $
 */
public class FileHeader implements LoggableObject, LogReadable {

    private static final int LOG_VERSION = 1;

    /* 
     * fileNum is the number of file, starting at 0. An unsigned
     * int, so stored in a long in memory, but in 4 bytes on disk
     */
    private long fileNum; 
    private long lastEntryInPrevFileOffset;
    private Timestamp time;
    private int logVersion;

    FileHeader(long fileNum, long lastEntryInPrevFileOffset) {
        this.fileNum = fileNum;
        this.lastEntryInPrevFileOffset = lastEntryInPrevFileOffset;
        Calendar now = Calendar.getInstance();
        time = new Timestamp(now.getTimeInMillis());
        logVersion = LOG_VERSION;
    }

    /** 
     * For logging only
     */
    public FileHeader() {
    }

    /**
     * @throws DatabaseException if the header isn't valid.
     */
    void validate(String fileName, long expectedFileNum) 
        throws DatabaseException {

        if (fileNum != expectedFileNum) {
            throw new LogException
                ("Wrong filenum in header for file " +
                 fileName + " expected " +
                 expectedFileNum + " got " + fileNum);
        }
    }

    /**
     * @return the offset of the last entry in the previous file.
     */
    long getLastEntryInPrevFileOffset() {
        return lastEntryInPrevFileOffset;
    }

    /*
     * Logging support
     */

    /**
     * @see LoggableObject#getLogType
     */
    public LogEntryType getLogType() {
        return LogEntryType.LOG_FILE_HEADER;
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
    public void postLogWork(DbLsn justLoggedLsn) 
        throws DatabaseException {
    }

    /**
     * A header is always a known size.
     */
    static int entrySize() {
        return
            LogUtils.getTimestampLogSize() + // time
            LogUtils.UNSIGNED_INT_BYTES +    // file number
            LogUtils.LONG_BYTES +            // lastEntryInPrevFileOffset
            LogUtils.INT_BYTES;              // logVersion
    }
    /**
     * @see LoggableObject#getLogSize
     * @return number of bytes used to store this object
     */
    public int getLogSize() {
        return entrySize();
    }            

    /**
     * @see LoggableObject#writeToLog
     * Serialize this object into the buffer. Update cksum with all
     * the bytes used by this object
     * @param logBuffer is the destination buffer
     */
    public void writeToLog(ByteBuffer logBuffer) {
        LogUtils.writeTimestamp(logBuffer, time);
        LogUtils.writeUnsignedInt(logBuffer,fileNum);
        LogUtils.writeLong(logBuffer, lastEntryInPrevFileOffset);
        LogUtils.writeInt(logBuffer, logVersion);
    }

    /**
     * @see LogReadable#readFromLog
     * Initialize this object from the data in itemBuf.
     * @param itemBuf the source buffer
     */
    public void readFromLog(ByteBuffer logBuffer)
	throws LogException {
        time = LogUtils.readTimestamp(logBuffer);
        fileNum = LogUtils.getUnsignedInt(logBuffer);
        lastEntryInPrevFileOffset = LogUtils.readLong(logBuffer);
        logVersion = LogUtils.readInt(logBuffer);
        if (logVersion != LOG_VERSION) {
            throw new LogException("Expected log version " + LOG_VERSION +
                                   " but found " + logVersion +
                                   " -- this version is not supported.");
        }
    }

    /**
     * @see LogReadable#dumpLog
     * @param sb destination string buffer
     * @param verbose if true, dump the full, verbose version
     */
    public void dumpLog(StringBuffer sb, boolean verbose) {
        sb.append("<FileHeader num=\"0x");
        sb.append(Long.toHexString(fileNum));
        sb.append("\" lastEntryInPrevFileOffset=\"0x");
        sb.append(Long.toHexString(lastEntryInPrevFileOffset));
        sb.append("\" logVersion=\"0x");
        sb.append(Integer.toHexString(logVersion));
        sb.append("\" time=\"").append(time);
        sb.append("\"/>");
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
     * Print in xml format
     */
    public String toString() {
        StringBuffer sb = new StringBuffer();
        dumpLog(sb, true);
        return sb.toString();
    }
     
}

