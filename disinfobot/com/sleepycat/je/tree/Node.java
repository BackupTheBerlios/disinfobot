/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: Node.java,v 1.1 2004/11/22 18:27:57 kate Exp $
*/

package com.sleepycat.je.tree;

import java.nio.ByteBuffer;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.INList;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogException;
import com.sleepycat.je.log.LogReadable;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.log.LogWritable;
import com.sleepycat.je.log.LoggableObject;
import com.sleepycat.je.utilint.DbLsn;

/**
 * A Node contains all the common base information for any JE B-Tree node.
 */
public abstract class Node
    implements LoggableObject, LogReadable, LogWritable {

    /*
     * The last allocated id. Note that nodeids will be shared
     * across db environments. lastAllocatedId must be initialized at
     * startup by Recovery.
     */
    public static void setLastNodeId(long id) {
	lastAllocatedId = id;
    }

    private static long lastAllocatedId = 0;

    private static final String BEGIN_TAG = "<node>";
    private static final String END_TAG = "</node>";

    // The unique id of this node
    private long nodeId;

    /**
     * Disallow use
     */
    private Node() {
    }

    /**
     * Create a new node, assigning it the next available node id.
     */
    protected Node(boolean init) {
        if (init) {
            nodeId = getNextNodeId();
        }
    }

    /**
     * Increment and return the next usable id. Must be synchronized.
     */
    private static synchronized long getNextNodeId() {
        return ++lastAllocatedId;
    }

    /**
     * Get the latest id, for checkpointing.
     */
    public static synchronized long getLastId() {
        return lastAllocatedId;
    }

    /**
     * Initialize a node that has been faulted in from the log
     */
    public void postFetchInit(DatabaseImpl db)
        throws DatabaseException {
        // nothing to do
    }

    public long getNodeId() {
	return nodeId;
    }

    /* For unit tests only. */
    void setNodeId(long nid) {
	nodeId = nid;
    }

    public void verify(Key maxKey)
	throws DatabaseException {
    }

    /**
     * @return true if this node is a duplicate-bearing node type, false
     * if otherwise.
     */
    public boolean containsDuplicates() {
	return false;
    }

    /**
     * Cover for LN's and just return 0 since they'll always be at the bottom
     * of the tree.
     */
    int getLevel() {
	return 0;
    }

    /*
     * Depth first search through a duplicate tree looking for an LN that
     * has nodeId.  When we find it, set location.bin and index and return
     * true.  If we don't find it, return false.
     *
     * No latching is performed.
     */
    boolean matchLNByNodeId(TreeLocation location, long nodeId)
	throws DatabaseException {

	throw new DatabaseException("matchLNByNodeId called on non DIN/DBIN");
    }

    /**
     * Add yourself to the in memory list if you're a type of node that 
     * should belong.
     */
    abstract void rebuildINList(INList inList)
        throws DatabaseException;

    /**
     * Remove yourself from the in memory list if you're a type of node that 
     * is put there.
     */
    abstract void removeFromINList(INList inList)
        throws DatabaseException;

    /**
     * @return true if you're part of a deletable subtree.
     */
    abstract boolean isValidForDelete()
        throws DatabaseException;

    /**
     * @return true if you're an IN in the search path
     */
    abstract protected boolean isSoughtNode(long nid) 
        throws DatabaseException;

    /**
     * @return true if you can be the ancestor of the target IN
     */
    abstract protected boolean canBeAncestor(IN target);

    /**
     * Return the approximate size of this node in memory, if this
     * size should be included in it's parents memory accounting. 
     * For example, all INs return 0, because they are accounted for 
     * individually. LNs must return a count, they're not counted on
     * the INList.
     */
    protected long getMemorySizeIncludedByParent() {
        return 0;
    }

    /*
     * Dumping
     */

    /**
     * Default toString method at the root of the tree.
     */
    public String toString() {
        return this.dumpString(0, true);
    }

    private String beginTag() {
	return BEGIN_TAG;
    }

    private String endTag() {
	return END_TAG;
    }

    public void dump(int nSpaces) {
	System.out.print(dumpString(nSpaces, true));
    }
    
    String dumpString(int nSpaces, boolean dumpTags) {
        StringBuffer self = new StringBuffer();
        self.append(TreeUtils.indent(nSpaces));
	if (dumpTags) {
	    self.append(beginTag());
	}
        self.append(nodeId);
	if (dumpTags) {
	    self.append(endTag());
	}
        return self.toString();
    }

    public String shortDescription() {
	return "<" + getType() + "/" + getNodeId();
    }

    public String getType() {
	return getClass().getName();
    }

    /* 
     * Logging support
     */

    /**
     * @see LoggableObject#getLogType
     */
    public abstract LogEntryType getLogType();

    /**
     * @see LoggableObject#marshallOutsideWriteLatch
     * By default, nodes can be marshalled outside the log write latch.
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
     * @see LoggableObject#getLogSize
     */
    public int getLogSize() {
        return LogUtils.LONG_BYTES;
    }

    /**
     * @see LogWritable#writeToLog
     */
    public void writeToLog(ByteBuffer logBuffer) {
        LogUtils.writeLong(logBuffer, nodeId);
    }

    /**
     * @see LogReadable#readFromLog
     */
    public void readFromLog(ByteBuffer itemBuffer)
	throws LogException {

        nodeId = LogUtils.readLong(itemBuffer);
    }

    /**
     * @see LogReadable#dumpLog
     */
    public void dumpLog(StringBuffer sb, boolean verbose) {
        sb.append(BEGIN_TAG);
        sb.append(nodeId);
        sb.append(END_TAG);
    }

    /**
     * The node Id is always the first item in a node. This method used
     * for quick scanning of the log to calculate maximum node id
     */
    public static long readNodeIdFromLog(ByteBuffer itemBuffer,
                                         int entrySize) {
        long nodeId = LogUtils.readLong(itemBuffer);
        itemBuffer.position(itemBuffer.position()+ entrySize -
                            LogUtils.LONG_BYTES);
        return nodeId;
    }
}
