/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: DIN.java,v 1.1 2004/11/22 18:27:57 kate Exp $
*/

package com.sleepycat.je.tree;

import java.nio.ByteBuffer;
import java.util.Comparator;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogException;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.txn.LockResult;
import com.sleepycat.je.txn.Locker;
import com.sleepycat.je.utilint.DbLsn;

/**
 * An DIN represents an Duplicate Internal Node in the JE tree.
 */
public final class DIN extends IN {

    private static final String BEGIN_TAG = "<din>";
    private static final String END_TAG = "</din>";

    /**
     * Full key for this set of duplicates.
     */
    private Key dupKey;

    /**
     * Reference to DupCountLN which stores the count.
     */
    private ChildReference dupCountLNRef;

    /**
     * Create an empty DIN, with no node id, to be filled in from the log.
     */
    public DIN() {
        super(); 

        dupKey = new Key();
        dupCountLNRef = new ChildReference();
        init(null, new Key(), 0, 0);
    }

    /**
     * Create a new DIN.
     */
    public DIN(DatabaseImpl db, Key identifierKey, int capacity,
               Key dupKey, ChildReference dupCountLNRef, int level) {
        super(db, identifierKey, capacity, level);

        this.dupKey = dupKey;
        this.dupCountLNRef = dupCountLNRef;
        initMemorySize(); // init after adding dup count ln */
    }

    /* Duplicates have no mask on their levels. */
    protected int generateLevel(DatabaseId dbId, int newLevel) {
        return newLevel;
    }

    /**
     * Create a new DIN.  Need this because we can't call newInstance()
     * without getting a 0 node.
     */
    protected IN createNewInstance(Key identifierKey,
                                   int maxEntries,
                                   int level) {
        return new DIN(getDatabase(),
                       identifierKey,
                       maxEntries,
                       dupKey,
                       dupCountLNRef,
                       level);
    }

    /**
     * Return the key for this duplicate set.
     */
    public Key getDupKey() {
        return dupKey;
    }

    /**
     * Get the key (dupe or identifier) in child that is used to locate
     * it in 'this' node.
     */
    public Key getChildKey(IN child)
        throws DatabaseException {

        return child.getIdentifierKey();
    }

    /*
     * A DIN uses the dupTree key in its searches.
     */
    public Key selectKey(Key mainTreeKey, Key dupTreeKey) {
        return dupTreeKey;
    }

    /**
     * Return the key for navigating through the duplicate tree.
     */
    Key getDupTreeKey() {
        return getIdentifierKey();
    }
    /**
     * Return the key for navigating through the main tree.
     */
    public Key getMainTreeKey() {
        return dupKey;
    }

    public ChildReference getDupCountLNRef() {
        return dupCountLNRef;
    }

    public DupCountLN getDupCountLN() 
        throws DatabaseException {

        return (DupCountLN) dupCountLNRef.fetchTarget(getDatabase(), this);
    }

    /*
     * All methods that modify the dup count ln must adjust memory sizing.
     */

    /**
     * Assign the dup count ln.
     */
    void setDupCountLN(ChildReference dupCountLNRef) {
        updateMemorySize(this.dupCountLNRef, dupCountLNRef);
        this.dupCountLNRef = dupCountLNRef;
    }

    /**
     * Update dup count ln.
     */
    public void updateDupCountLNRefAndNullTarget(DbLsn newLsn) {
        setDirty(true);
        long oldSize = dupCountLNRef.getInMemorySize();
        dupCountLNRef.setTarget(null);
        dupCountLNRef.setLsn(newLsn);
        long newSize = dupCountLNRef.getInMemorySize();
        updateMemorySize(oldSize, newSize);
    }

    /**
     * Update dup count lsn.
     */
    public void updateDupCountLNRef(DbLsn newLsn) {
        setDirty(true);
        updateMemorySize(dupCountLNRef.getLsn(), newLsn);
        dupCountLNRef.setLsn(newLsn);
    }

    /**
     * @return true if this node is a duplicate-bearing node type, false
     * if otherwise.
     */
    public boolean containsDuplicates() {
        return true;
    }

    /* Never true for a DIN. */
    public boolean isDbRoot() {
	return false;
    }

    /**
     * Return the comparator function to be used for DINs.  This is
     * the user defined duplicate comparison function, if defined.
     */
    public final Comparator getKeyComparator() {
        return getDatabase().getDuplicateComparator();
    }

    void incrementDuplicateCount(EnvironmentImpl env,
                                 LockResult lockResult,
                                 Key key,
                                 Locker locker)
        throws DatabaseException {

        /* 
         * Increment the duplicate count and update its owning
         * DBIN.
         */
        DbLsn oldLsn = dupCountLNRef.getLsn();
        lockResult.setAbortLsn(oldLsn,
                               dupCountLNRef.isKnownDeleted());
        DupCountLN dupCountLN = getDupCountLN();
        dupCountLN.incDupCount();
        DbLsn newCountLSN = 
            dupCountLN.log(env, getDatabase().getId(),
                           key, oldLsn, locker);
        updateDupCountLNRef(newCountLSN);
    }

    /**
     * Count up the memory usage attributable to this node alone. LNs children
     * are counted by their BIN/DIN parents, but INs are not counted by 
     * their parents because they are resident on the IN list.
     */
    protected long computeMemorySize() {
        long size = super.computeMemorySize();
        if (dupCountLNRef != null) {
            size += dupCountLNRef.getInMemorySize();
        }
        return size;
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

        for (int i = 0; i < getNEntries(); i++) {
	    ChildReference ref = getEntry(i);
	    if (ref != null) {
		Node n = ref.fetchTarget(getDatabase(), this);
		if (n != null) {
		    boolean ret = n.matchLNByNodeId(location, nodeId);
		    if (ret) {
			return true;
		    }
		}
	    }
        }

	return false;
    }

    /*
     * DbStat support.
     */
    void accumulateStats(TreeWalkerStatsAccumulator acc) {
	acc.processDIN(this, new Long(getNodeId()), getLevel());
    }

    /*
     * Logging Support
     */

    /**
     * @see IN#getLogType
     */
    public LogEntryType getLogType() {
        return LogEntryType.LOG_DIN;
    }

    /**
     * @see IN#getLogSize
     */
    public int getLogSize() {
        int size = super.getLogSize();          // ancestors
        size += dupKey.getLogSize();            // identifier key
        size += LogUtils.getBooleanLogSize(); // dupCountLNRef null flag
        if (dupCountLNRef != null) {
            size += dupCountLNRef.getLogSize();
        }
        return size;
    }

    /**
     * @see IN#writeToLog
     */
    public void writeToLog(ByteBuffer logBuffer) {

        // ancestors
        super.writeToLog(logBuffer); 

        // identifier key
        dupKey.writeToLog(logBuffer);

        /* DupCountLN */
        boolean dupCountLNRefExists = (dupCountLNRef != null);
        LogUtils.writeBoolean(logBuffer, dupCountLNRefExists);
        if (dupCountLNRefExists) {
            dupCountLNRef.writeToLog(logBuffer);    
        }
    }

    /**
     * @see IN#readFromLog
     */
    public void readFromLog(ByteBuffer itemBuffer)
        throws LogException {

        // ancestors 
        super.readFromLog(itemBuffer);

        // identifier key
        dupKey.readFromLog(itemBuffer);

        /* DupCountLN */
        boolean dupCountLNRefExists = LogUtils.readBoolean(itemBuffer);
        if (dupCountLNRefExists) {
            dupCountLNRef.readFromLog(itemBuffer);
        } else {
            dupCountLNRef = null;
        }
    }
    
    /**
     * DINS need to dump their dup key
     */
    protected void dumpLogAdditional(StringBuffer sb) {
        super.dumpLogAdditional(sb);
        dupKey.dumpLog(sb, true);
        if (dupCountLNRef != null) {
            dupCountLNRef.dumpLog(sb, true);
        }
    }

    /*
     * Dumping
     */

    public String beginTag() {
        return BEGIN_TAG;
    }

    public String endTag() {
        return END_TAG;
    }

    /**
     * For unit test support:
     * @return a string that dumps information about this DIN, without
     */
    public String dumpString(int nSpaces, boolean dumpTags) {
        StringBuffer sb = new StringBuffer();
        if (dumpTags) {
            sb.append(TreeUtils.indent(nSpaces));
            sb.append(beginTag());
            sb.append('\n');
        }

        sb.append(TreeUtils.indent(nSpaces+2));
        sb.append("<dupkey>");
        sb.append((dupKey == null ? "" : dupKey.toString()));
        sb.append("</dupkey>");
        sb.append('\n');
        if (dupCountLNRef == null) {
	    sb.append(TreeUtils.indent(nSpaces+2));
            sb.append("<dupCountLN/>");
        } else {
            sb.append(dupCountLNRef.dumpString(nSpaces + 4, true));
        }
        sb.append('\n');
        sb.append(super.dumpString(nSpaces, false));

        if (dumpTags) {
            sb.append(TreeUtils.indent(nSpaces));
            sb.append(endTag());
        }
        return sb.toString();
    }

    public String toString() {
        return dumpString(0, true);
    }

    public String shortClassName() {
        return "DIN";
    }
}
