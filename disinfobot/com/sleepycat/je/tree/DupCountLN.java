/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: DupCountLN.java,v 1.1 2004/11/22 18:27:57 kate Exp $
*/

package com.sleepycat.je.tree;

import java.nio.ByteBuffer;

import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogException;
import com.sleepycat.je.log.LogUtils;

/**
 * A DupCountLN represents the transactional part of the root of a
 * duplicate tree, specifically the count of dupes in the tree.
 */
public final class DupCountLN extends LN {

    private static final String BEGIN_TAG = "<dupCountLN>";
    private static final String END_TAG = "</dupCountLN>";

    private int dupCount;

    /**
     * Create a new DupCountLn to hold a new DIN.
     */
    public DupCountLN(int count) {
        super(new byte[0]);

        /*
         * This ctor is always called from Tree.createDuplicateEntry
         * where there will be one existing LN and a new dup LN being
         * inserted to create the new duplicate tree.  So the minimum
         * starting point for a duplicate tree is 2 entries.
         */
        this.dupCount = count;
    }

    /**
     * Create an empty DupCountLN, to be filled in from the log.
     */
    public DupCountLN() {
        super();
        dupCount = 0;
    }

    public int getDupCount() {
        return dupCount;
    }

    public int incDupCount() {
        dupCount++;
        assert dupCount >= 0;
        return dupCount;
    }

    public int decDupCount() {
        dupCount--;
        assert dupCount >= 0;
        return dupCount;
    }

    void setDupCount(int dupCount) {
        this.dupCount = dupCount;
    }

    /**
     * @return true if this node is a duplicate-bearing node type, false
     * if otherwise.
     */
    public boolean containsDuplicates() {
        return true;
    }

    public boolean isDeleted() {
        return false;
    }

    /**
     * Compute the approximate size of this node in memory for evictor
     * invocation purposes.
     */
    protected long computeInMemorySize() {
        return 0;
    }

    /*
     * DbStat support.
     */
    public void accumulateStats(TreeWalkerStatsAccumulator acc) {
	acc.processDupCountLN(this, new Long(getNodeId()));
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
        if (dumpTags) {
            sb.append(TreeUtils.indent(nSpaces));
            sb.append(beginTag());
            sb.append('\n');
        }
        sb.append(TreeUtils.indent(nSpaces+2));
        sb.append("<count v=\"").append(dupCount).append("\"/>").append('\n');
        sb.append(super.dumpString(nSpaces, false));
        if (dumpTags) {
            sb.append(TreeUtils.indent(nSpaces));
            sb.append(endTag());
        }
        return sb.toString();
    }

    /*
     * Logging
     */

    /**
     * Log type for transactional entries.
     */
    protected LogEntryType getTransactionalLogType() {
        return LogEntryType.LOG_DUPCOUNTLN_TRANSACTIONAL;
    }

    /**
     * @see LN#getLogType
     */
    public LogEntryType getLogType() {
        return LogEntryType.LOG_DUPCOUNTLN;
    }

    /**
     * @see LN#getLogSize
     */
    public int getLogSize() {
        return super.getLogSize() +
            LogUtils.INT_BYTES;
    }

    /**
     * @see LN#writeToLog
     */
    public void writeToLog(ByteBuffer logBuffer) {
        // Ask ancestors to write to log
        super.writeToLog(logBuffer); 
        LogUtils.writeInt(logBuffer, dupCount);
    }

    /**
     * @see LN#readFromLog
     */
    public void readFromLog(ByteBuffer itemBuffer)
        throws LogException {

        super.readFromLog(itemBuffer);
        dupCount = LogUtils.readInt(itemBuffer);
    }

    /**
     * Dump additional fields
     */
    protected void dumpLogAdditional(StringBuffer sb) {
        super.dumpLogAdditional(sb);
        sb.append("<count v=\"").append(dupCount).append("\"/>");
    }
}
