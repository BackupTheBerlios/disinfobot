/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: NameLN.java,v 1.1 2004/11/22 18:27:57 kate Exp $
*/

package com.sleepycat.je.tree;

import java.nio.ByteBuffer;

import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogException;
import com.sleepycat.je.log.LogUtils;

/**
 * A NameLN represents a Leaf Node in the name->database id mapping tree.
 */
public final class NameLN extends LN {

    private static final String BEGIN_TAG = "<nameLN>";
    private static final String END_TAG = "</nameLN>";

    private DatabaseId id;
    private boolean deleted;

    /**
     * In the ideal world, we'd have a base LN class so that this NameLN
     * doesn't have a superfluous data field, but we want to optimize the LN
     * class for size and speed right now.
     */
    public NameLN(DatabaseId id) {
        super(new byte[0]);
        this.id = id;
        deleted = false;
    }

    /**
     * Create an empty NameLN, to be filled in from the log.
     */
    public NameLN() {
        super();
        id = new DatabaseId();
    }

    public boolean isDeleted() {
        return deleted;
    }

    void makeDeleted() {
        deleted = true;
    }

    public DatabaseId getId() {
        return id;
    }

    public void setId(DatabaseId id) {
        this.id = id;
    }

    /**
     * Compute the approximate size of this node in memory for evictor
     * invocation purposes.
     */
    protected long computeInMemorySize() {
        return 0;
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
        sb.append(TreeUtils.indent(nSpaces));
        sb.append("<deleted val=\"").append(Boolean.toString(deleted));
        sb.append("\">");
        sb.append('\n');
        sb.append(TreeUtils.indent(nSpaces));
        sb.append("<id val=\"").append(id);
        sb.append("\">");
        sb.append('\n');
        return sb.toString();
    }

    /*
     * Logging
     */

    /**
     * Log type for transactional entries.
     */
    protected LogEntryType getTransactionalLogType() {
        return LogEntryType.LOG_NAMELN_TRANSACTIONAL;
    }

    /**
     * @see LN#getLogType
     */
    public LogEntryType getLogType() {
        return LogEntryType.LOG_NAMELN;
    }

    /**
     * @see LN#getLogSize
     */
    public int getLogSize() {
        return
            super.getLogSize() +                     // superclass
            id.getLogSize() +                        // id
            LogUtils.getBooleanLogSize();            // deleted flag
    }

    /**
     * @see LN#writeToLog
     */
    public void writeToLog(ByteBuffer logBuffer) {
        /* Ask ancestors to write to log. */
        super.writeToLog(logBuffer);         // super class
        id.writeToLog(logBuffer);            // id
        LogUtils.writeBoolean(logBuffer, deleted); // deleted flag
    }

    /**
     * @see LN#readFromLog
     */
    public void readFromLog(ByteBuffer itemBuffer)
        throws LogException {

        super.readFromLog(itemBuffer);                // super class
        id.readFromLog(itemBuffer);                   // id
        deleted = LogUtils.readBoolean(itemBuffer);   // deleted flag
    }

    /**
     * Dump additional fields. Done this way so the additional info can be
     * within the XML tags defining the dumped log entry.
     */
    protected void dumpLogAdditional(StringBuffer sb) {
        id.dumpLog(sb, true);
    }
}
