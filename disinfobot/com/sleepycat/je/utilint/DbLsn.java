/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: DbLsn.java,v 1.1 2004/11/22 18:27:58 kate Exp $
*/

package com.sleepycat.je.utilint;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.sleepycat.je.log.FileManager;
import com.sleepycat.je.log.LogReadable;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.log.LogWritable;
import com.sleepycat.je.tree.TreeUtils;

/**
 * DbLsn is a class that represents a Log Sequence Number (Lsn) A LSN is a file
 * number (32b) and offset within that file (32b) which references a unique
 * record in the database environment log.  We hide the actual implementation
 * behind an abstraction and return longs so that we don't have to worry about
 * the lack of unsigned quantities.  DbLsn's are immutable.
 */
public class DbLsn implements Comparable, LogWritable, LogReadable {
    static final long INT_MASK = 0xFFFFFFFFL;

    private long lsn;

    public static final DbLsn NULL_LSN = new DbLsn(-1, 0);

    public DbLsn() {
        lsn = -1;
    }

    /**
     * Construct a new DbLsn from a file number and file offset.
     */

    public DbLsn(long fileNumber, long fileOffset) {
	lsn = fileOffset & INT_MASK |
	    ((fileNumber & INT_MASK) << 32);
    }

    /**
     * Return the file number for this DbLsn.
     * @return the number for this DbLsn.
     */

    public long getFileNumber() {
	return (lsn >> 32) & INT_MASK;
    }

    /**
     * Return the file offset for this DbLsn.
     * @return the offset for this DbLsn.
     */

    public long getFileOffset() {
	return (lsn & INT_MASK);
    }

    private int compareLong(long l1, long l2) {
	if (l1 < l2) {
	    return -1;
	} else if (l1 > l2) {
	    return 1;
	} else {
	    return 0;
	}
    }

    /**
     * see Comparable#compareTo
     */
    public int compareTo(Object o) {
	if (o == null ||
	    o.equals(NULL_LSN) ||
	    this.equals(NULL_LSN)) {
	    throw new NullPointerException();
	}

        DbLsn argLsn = (DbLsn) o;
        long argFileNumber = argLsn.getFileNumber();
        long thisFileNumber = getFileNumber();
        if (argFileNumber == thisFileNumber) {
            return compareLong(getFileOffset(), argLsn.getFileOffset());
        } else {
            return compareLong(thisFileNumber, argFileNumber);
        }
    }

    public String toString() {
	return "<DbLsn val=\"0x" + 
            Long.toHexString(getFileNumber()) +
            "/0x" +
            Long.toHexString(getFileOffset()) +
	    "\"/>";
    }

    public String getNoFormatString() {
        return "0x" + Long.toHexString(getFileNumber()) + "/0x" +
            Long.toHexString(getFileOffset());
    }

    public String dumpString(int nSpaces) {
        StringBuffer sb = new StringBuffer();
        sb.append(TreeUtils.indent(nSpaces));
        sb.append(toString());
        return sb.toString();
    }

    /**
     *  Just in case it's ever used as a hash key
     */
    public int hashCode() {
        return new Long(lsn).hashCode();
    }

    /**
     * Return the logsize in bytes between these two lsn. This is an
     * approximation; the logs might actually be a little more or less in
     * size. This assumes that no log files have been cleaned.
     */
    public long getNoCleaningDistance(DbLsn other, long logFileSize) {
        long diff = 0;

        /* First figure out how many files lay between the two. */
        long myFile = getFileNumber();
        if (other == null) {
            other = new DbLsn(0, 0);
        }
        long otherFile = other.getFileNumber();
        if (myFile == otherFile) {
            diff = Math.abs(getFileOffset() - other.getFileOffset());
        } else if (myFile > otherFile) {
            diff = calcDiff(myFile-otherFile,
                            logFileSize, this, other);
        } else {
            diff = calcDiff(otherFile - myFile,
                            logFileSize, other, this);
        }
        return diff;
    }
        
    /**
     * Return the logsize in bytes between these two lsn. This is an
     * approximation; the logs might actually be a little more or less in
     * size. This assumes that log files might have been cleaned.
     */
    public long getWithCleaningDistance(FileManager fileManager,
                                        DbLsn other,
                                        long logFileSize) {
        long diff = 0;

        /* First figure out how many files lay between the two. */
        long myFile = getFileNumber();
        if (other == null) {
            other = new DbLsn(0, 0);
        }
        long otherFile = other.getFileNumber();
        if (myFile == otherFile) {
            diff = Math.abs(getFileOffset() - other.getFileOffset());
        } else {
            /* Figure out how many files lie between. */
            Long [] fileNums = fileManager.getAllFileNumbers();
            int myFileIdx = Arrays.binarySearch(fileNums, 
                                                new Long(myFile));
            int otherFileIdx = Arrays.binarySearch(fileNums, 
                                                   new Long(otherFile));
            if (myFileIdx > otherFileIdx) {
                diff = calcDiff(myFileIdx-otherFileIdx,
                                logFileSize, this, other);
            } else {
                diff = calcDiff(otherFileIdx - myFileIdx,
                                logFileSize, other, this);
            }
        }
        return diff;
    }

    private long calcDiff(long fileDistance, 
                          long logFileSize,
                          DbLsn laterLsn,
                          DbLsn earlierLsn) {
        long diff = fileDistance * logFileSize;
        diff += laterLsn.getFileOffset();
        diff -= earlierLsn.getFileOffset();
        return diff;
    }

    /**
     * Override Object.equals
     */
    public boolean equals(Object obj) {
        // Same instance?
        if (this == obj) {
            return true;
        }

        // Is it another DbLsn?
        if (!(obj instanceof DbLsn)) {
            return false;
        } else {
            return (lsn == ((DbLsn) obj).lsn);
        }
    }

    /**
     * @see LogWritable#getLogSize
     */
    public int getLogSize() {
        return LogUtils.getLongLogSize();
    }

    /**
     * @see LogWritable#writeToLog
     */
    public void writeToLog(ByteBuffer logBuffer) {
        LogUtils.writeLong(logBuffer,lsn);
    }

    /**
     * @see LogReadable#readFromLog
     */
    public void readFromLog(ByteBuffer itemBuffer) {
        lsn = LogUtils.readLong(itemBuffer);
    }

    /**
     * @see LogReadable#dumpLog
     */
    public void dumpLog(StringBuffer sb, boolean verbose) {
        sb.append(toString());
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
}
