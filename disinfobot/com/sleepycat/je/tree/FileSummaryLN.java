/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: FileSummaryLN.java,v 1.1 2004/11/22 18:27:57 kate Exp $
*/

package com.sleepycat.je.tree;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.cleaner.FileSummary;
import com.sleepycat.je.cleaner.TrackedFileSummary;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogException;

/**
 * A FileSummaryLN represents a Leaf Node in the UtilizationProfile database. 
 * 
 * <p>The contents of the FileSummaryLN are not fixed until the moment at which
 * the LN is added to the log.  A base summary object contains the summary last
 * added to the log.  A tracked summary object contains live summary info being
 * updated in real time.  The tracked summary is added to the base summary just
 * before logging it, and then the tracked summary is reset.  This ensures that
 * the logged summary will accurately reflect the totals calculated at the
 * point in the log where the LN is added.</p>
 *
 * <p>This is all done in the writeToLog method, which operates under the log
 * write latch.  All utilization tracking must be done under the log write
 * latch.</p>
 */
public final class FileSummaryLN extends LN {

    private static final String BEGIN_TAG = "<fileSummaryLN>";
    private static final String END_TAG = "</fileSummaryLN>";

    private FileSummary baseSummary;
    private TrackedFileSummary trackedSummary;

    /**
     * Creates a new LN with a given base summary.
     */
    public FileSummaryLN(FileSummary baseSummary) {
        super(new byte[0]);
        assert baseSummary != null;
        this.baseSummary = baseSummary;
    }

    /**
     * Creates an empty LN to be filled in from the log.
     */
    public FileSummaryLN() 
        throws DatabaseException {
        baseSummary = new FileSummary();
    }

    /**
     * Sets the live summary object that will be added to the base summary at
     * the time the LN is logged.
     */
    public void setTrackedSummary(TrackedFileSummary trackedSummary) {
        this.trackedSummary = trackedSummary;
    }

    /**
     * Returns the tracked summary, or null if setTrackedSummary was not
     * called.
     */
    public TrackedFileSummary getTrackedSummary() {
        return trackedSummary;
    }

    /**
     * Returns the base summary for the file that is stored in the LN.
     */
    public FileSummary getBaseSummary() {
        return baseSummary;
    }

    /**
     * Initialize a node that has been faulted in from the log
     */
    public void postFetchInit(DatabaseImpl db) 
        throws DatabaseException {
        // nothing to do
    }

    /**
     * Compute the approximate size of this node in memory for evictor
     * invocation purposes.
     */
    protected long computeInMemorySize() {
        return 0; // XXX
    }

    /**
     * Convert a FileSummaryLN key from a byte array to a long.
     */
    public static long bytesToFileNumber(byte[] bytes) {
        try {
            return Long.valueOf(new String(bytes, "UTF-8")).longValue();
        } catch (UnsupportedEncodingException shouldNeverHappen) {
            assert false: shouldNeverHappen;
            return 0;
        }
    }

    /**
     * Convert a FileSummaryLN key from a long to a byte array.
     */
    public static byte[] fileNumberToBytes(long fileNum) {
        try {
            return String.valueOf(fileNum).getBytes("UTF-8");
        } catch (UnsupportedEncodingException shouldNeverHappen) {
            assert false: shouldNeverHappen;
            return null;
        }
    }

    /*
     * Dumping
     */

    public String toString() {
        return dumpString(0, true);
    }
    
    public String beginTag() {
        return BEGIN_TAG;
    }

    public String endTag() {
        return END_TAG;
    }

    public String dumpString(int nSpaces, boolean dumpTags) {
        StringBuffer sb = new StringBuffer();
        sb.append(super.dumpString(nSpaces, dumpTags));
        sb.append('\n');
        if (!isDeleted()) {
            sb.append(baseSummary.toString());
        }
        return sb.toString();
    }

    /**
     * Dump additional fields. Done this way so the additional info can
     * be within the XML tags defining the dumped log entry.
     */
    protected void dumpLogAdditional(StringBuffer sb) {
        sb.append(baseSummary.toString());
    }

    /*
     * Logging
     */

    /**
     * Log type for transactional entries.
     */
    protected LogEntryType getTransactionalLogType() {
        assert false : "Txnl access to UP db not allowed";
        return LogEntryType.LOG_FILESUMMARYLN;
    }

    /**
     * @see LN#getLogType
     */
    public LogEntryType getLogType() {
        return LogEntryType.LOG_FILESUMMARYLN;
    }

    /**
     * @see LoggableObject#marshallOutsideWriteLatch
     * FileSummaryLNs must be marshalled within the log write latch, because
     * that critical section is used to guarantee that all previous log
     * entries are reflected in the summary.
     */
    public boolean marshallOutsideWriteLatch() {
        return false;
    }

    /**
     * @see LN#getLogSize
     */
    public int getLogSize() {
        int size = super.getLogSize();
        if (!isDeleted()) {
            size += baseSummary.getLogSize();
        }
        return size;
    }

    /**
     * @see LN#writeToLog
     */
    public void writeToLog(ByteBuffer logBuffer) {

        /*
         * Add the tracked (live) summary to the base summary before writing
         * it to the log, and reset the tracked summary.
         */
        if (trackedSummary != null) {
            baseSummary.add(trackedSummary);
            trackedSummary.reset();
        }
        super.writeToLog(logBuffer); 
        if (!isDeleted()) {
            baseSummary.writeToLog(logBuffer);
        }
    }

    /**
     * @see LN#readFromLog
     */
    public void readFromLog(ByteBuffer itemBuffer)
        throws LogException {

        super.readFromLog(itemBuffer);
        if (!isDeleted()) {
            baseSummary.readFromLog(itemBuffer);
        }
    }
}
