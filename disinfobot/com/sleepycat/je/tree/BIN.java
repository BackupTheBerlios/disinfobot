/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: BIN.java,v 1.1 2004/11/22 18:27:57 kate Exp $
*/

package com.sleepycat.je.tree;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.CursorImpl;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.MemoryBudget;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogManager;
import com.sleepycat.je.log.LoggableObject;
import com.sleepycat.je.log.entry.INLogEntry;
import com.sleepycat.je.txn.BasicLocker;
import com.sleepycat.je.txn.LockGrantType;
import com.sleepycat.je.txn.Locker;
import com.sleepycat.je.utilint.DbLsn;

/**
 * A BIN represents a Bottom Internal Node in the JE tree.  
 */
public class BIN extends IN implements LoggableObject {

    private static final String BEGIN_TAG = "<bin>";
    private static final String END_TAG = "</bin>";

    /*
     * The set of cursors that are currently referring to this BIN.
     */
    protected Set cursorSet;

    /*
     * Support for logging BIN deltas. (Partial BIN logging)
     */
    private DbLsn lastFullVersion;      // location of last full version.
    private DbLsn lastDeltaVersion;     // location of last delta, for cleaning
    private int numDeltasSinceLastFull; // num deltas logged
    private boolean prohibitNextDelta;  // disallow delta on next log

    public BIN() {
        cursorSet = new HashSet();
        numDeltasSinceLastFull = 0;
        prohibitNextDelta = false;
    }

    public BIN(DatabaseImpl db,
	       Key identifierKey,
	       int maxEntriesPerNode,
	       int level) {
        super(db, identifierKey, maxEntriesPerNode, level);

        cursorSet = new HashSet();
        numDeltasSinceLastFull = 0;
        prohibitNextDelta = false;
    }


    /**
     * Create a holder object that encapsulates information about this
     * BIN for the INCompressor.
     */
    public BINReference createReference() {
        return new BINReference(getNodeId(), getDatabase().getId(),
                                getIdentifierKey());
    }

    /**
     * Create a new BIN.  Need this because we can't call newInstance()
     * without getting a 0 for nodeid.
     */
    protected IN createNewInstance(Key identifierKey,
				   int maxEntries,
				   int level) {
        return new BIN(getDatabase(), identifierKey, maxEntries, level);
    }

    /**
     * Get the key (dupe or identifier) in child that is used to locate
     * it in 'this' node.  For BIN's, the child node has to be a DIN
     * so we use the Dup Key to cross the main-tree/dupe-tree boundary.
     */
    public Key getChildKey(IN child)
        throws DatabaseException {

        return child.getDupKey();
    }
    
    /**
     * @return the log entry type to use for bin delta log entries.
     */
    public LogEntryType getBINDeltaType() {
        return LogEntryType.LOG_BIN_DELTA;
    }   
        
    /**
     * @return location of last logged full version. If never set, return null.
     */
    DbLsn getLastFullVersion() {
        return lastFullVersion;
    }

    /**
     * @return location of last logged delta version. If never set,
     * return null.
     */
    public DbLsn getLastDeltaVersion() {
        return lastDeltaVersion;
    }

    /**
     * For nodes that must remember their last full lsn. INs don't need to
     * remember.
     */
    protected void setLastFullLsn(DbLsn lsn) {
        lastFullVersion = lsn;
    }

    /**
     * If compressed, must log full version.
     * Overrides base class method.
     */
    protected void setCompressedSinceLastLog() {
        prohibitNextDelta = true;
    }

    /**
     * If cleaned, must log full version.
     * Overrides base class method.
     */
    public void setCleanedSinceLastLog() {
        prohibitNextDelta = true;
    }

    /*
     * If this search can go further, return the child. If it can't, and you
     * are a possible new parent to this child, return this IN. If the 
     * search can't go further and this IN can't be a parent to this child,
     * return null.
     */
    protected void descendOnParentSearch(SearchResult result,
                                         IN target,
                                         Node child,
                                         boolean requireExactMatch)
        throws DatabaseException {
        if (child.canBeAncestor(target)) {
            if (target.containsDuplicates() && target.isRoot()) {

                /* 
                 * Don't go further -- the target is a root of a dup tree, so
                 * this BIN will have to be the parent. 
                 */
                long childNid = child.getNodeId();
                ((IN) child).releaseLatch();

                result.keepSearching = false;           // stop searching

                if (childNid  == target.getNodeId()) {  // set if exact find
                    result.exactParentFound = true;
                } else {
                    result.exactParentFound = false;
                }

                /* 
                 * Return a reference to this node unless we need an exact
                 * match and this isn't exact.
                 */
                if (requireExactMatch && ! result.exactParentFound) {
                    result.parent = null;    
                    releaseLatch();      
                } else {
                    result.parent = this;
                }

            } else {
                /*
                 * Go further down into the dup tree.
                 */
                releaseLatch();
                result.parent = (IN) child;
            }
        } else {
            /*
             * Our search ends, we didn't find it. If we need an exact match,
             * give up, if we only need a potential match, keep this node
             * latched and return it.
             */
            result.exactParentFound = false;
            result.keepSearching = false;
            if (!requireExactMatch && target.containsDuplicates()) {
                result.parent = this;
            } else {
                releaseLatch();
                result.parent = null;
            }
        }
    }

    /* 
     * A BIN can be the ancestor of a LN or an internal node of the duplicate
     * tree. It can't be the parent of an IN or another BIN.
     */
    protected boolean canBeAncestor(IN target) {
        return target.containsDuplicates();
    }
    
    /*
     * An BIN is evictable if:
     *       - it is not pinned by the cleaner.
     *       - if it has no resident cursors.
     *       - if it has no resident non-LN children. That's because we must
     *         we can't be assured that the resident children are flushed.
     */
    public boolean isEvictable() {
        if (evictionProhibited) {
            return false;
        }

        if (nCursors() > 0) {
            return false;
        }

        return childrenAreEvictable();
    }

    protected boolean childrenAreEvictable() {
        for (int i = 0; i < getNEntries(); i++) {
            ChildReference ref = getEntry(i);

            if (ref.getTarget() != null) {
                if (!(ref.getTarget() instanceof LN)) {
                    /* A resident, nonLN child exists, not evictable. */
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Indicates whether entry 0's key is "special" in that it always
     * compares less than any other key.  BIN's don't have the special
     * key, but IN's do.
     */
    boolean entryZeroKeyComparesLow() {
        return false;
    }

    /**
     * @param index indicates target entry
     * @return true if this entry is deleted for sure. If this method returns
     * false, it means we're not sure about the record status and must check
     * the LN itself.
     */
    public boolean isKnownDeleted(int index) {
        ChildReference entry = getEntry(index);
        return (entry == null) || entry.isKnownDeleted();
    }

    /**
     * Mark this entry as deleted, using the delete flag. Only BINS
     * may do this.
     * @param index indicates target entry
     */
    public void setKnownDeleted(int index) {
        ChildReference entry = getEntry(index);
        entry.setKnownDeleted();
        updateMemorySize(entry.getTarget(), null);
        entry.setTarget(null);
        setDirty(true);
    }

    /**
     * Mark this entry as deleted, using the delete flag. Only BINS
     * may do this.  Don't null the target field.
     *
     * This is used so that an LN can still be locked by the compressor
     * even if the entry is knownDeleted.
     * See BIN.compress.
     *
     * @param index indicates target entry
     */
    public void setKnownDeletedLeaveTarget(int index) {
        ChildReference entry = getEntry(index);
        entry.setKnownDeleted();
        setDirty(true);
    }

    /**
     * Clear the known deleted flag. Only BINS may do this.
     * @param index indicates target entry
     */
    public void clearKnownDeleted(int index) {
        ChildReference entry = getEntry(index);
        entry.clearKnownDeleted();
        setDirty(true);
    }

    /* Called once at environment startup by MemoryBudget */
    public static long computeOverhead(DbConfigManager configManager) 
        throws DatabaseException {

        int capacity = configManager.getInt(EnvironmentParams.NODE_MAX);

        /* 
         * Overhead consists of all the fields in this class plus the
         * entries array.
         */
        return MemoryBudget.BIN_FIXED_OVERHEAD +
            (MemoryBudget.ARRAY_ITEM_OVERHEAD * capacity);
    }


    protected long getMemoryOverhead(MemoryBudget mb) {
        return mb.getBINOverhead();
    }

    /*
     * Cursors
     */

    /* public for the test suite. */
    public Set getCursorSet() {
        return cursorSet;
    }

    /**
     * Register a cursor with this bin.  Caller has this bin already latched.
     * @param cursor Cursor to register.
     */
    public void addCursor(CursorImpl cursor) {
        assert getLatch().isOwner();  
        cursorSet.add(cursor);
    }

    /**
     * Unregister a cursor with this bin.  Caller has this bin already
     * latched.
     *
     * @param cursor Cursor to unregister.
     */
    public void removeCursor(CursorImpl cursor) {
        assert getLatch().isOwner();
        cursorSet.remove(cursor);
    }

    /**
     * @return the number of cursors currently referring to this BIN.
     */
    public int nCursors() {
        return cursorSet.size();
    }

    /**
     * The following four methods access the correct fields in a
     * cursor depending on whether "this" is a BIN or DBIN.  For
     * BIN's, the CursorImpl.index and CursorImpl.bin fields should be
     * used.  For DBIN's, the CursorImpl.dupIndex and CursorImpl.dupBin
     * fields should be used.
     */
    BIN getCursorBIN(CursorImpl cursor) {
        return cursor.getBIN();
    }

    int getCursorIndex(CursorImpl cursor) {
        return cursor.getIndex();
    }

    void setCursorBIN(CursorImpl cursor, BIN bin) {
        cursor.setBIN(bin);
    }

    void setCursorIndex(CursorImpl cursor, int index) {
        cursor.setIndex(index);
    }

    /**
     * Called when we know we are about to split on behalf of a key
     * that is the minimum (leftSide) or maximum (!leftSide) of this
     * node.  This is achieved by just forcing the split to occur
     * either one element in from the left or the right
     * (i.e. splitIndex is 1 or nEntries - 1).
     */
    void splitSpecial(IN parent, int parentIndex, int maxEntriesPerNode,
		      Key key, boolean leftSide)
	throws DatabaseException {

	int index = findEntry(key, true, false);
	int nEntries = getNEntries();
	boolean exact = (index & IN.EXACT_MATCH) != 0;
	index &= ~IN.EXACT_MATCH;
	if (leftSide &&
	    index < 0) {
	    splitInternal(parent, parentIndex, maxEntriesPerNode, 1);
	} else if (!leftSide &&
		   !exact &&
		   index == (nEntries - 1)) {
	    splitInternal(parent, parentIndex, maxEntriesPerNode,
			  nEntries - 1);
	} else {
	    split(parent, parentIndex, maxEntriesPerNode);
	}
    }

    /**
     * Adjust any cursors that are referring to this BIN.  This method
     * is called during a split operation.  "this" is the BIN being split.
     * newSibling is the new BIN into which the entries from "this"
     * between newSiblingLow and newSiblingHigh have been copied.
     *
     * @param newSibling - the newSibling into which "this" has been split.
     * @param newSiblingLow, newSiblingHigh - the low and high entry of
     * "this" that were moved into newSibling.
     */
    void adjustCursors(IN newSibling,
                       int newSiblingLow,
                       int newSiblingHigh) {
        assert newSibling.getLatch().isOwner();
        assert this.getLatch().isOwner();
        int adjustmentDelta = (newSiblingHigh - newSiblingLow);
        Iterator iter = cursorSet.iterator();
        while (iter.hasNext()) {
            CursorImpl cursor = (CursorImpl) iter.next();
            int cIdx = getCursorIndex(cursor);
            BIN cBin = getCursorBIN(cursor);
            assert cBin == this;
            assert newSibling instanceof BIN;

            /*
             * There are four cases to consider for cursor adjustments,
             * depending on (1) how the existing node gets split, and
             * (2) where the cursor points to currently.
             * In cases 1 and 2, the id key of the node being split is
             * to the right of the splitindex so the new sibling gets
             * the node entries to the left of that index.  This is
             * indicated by "new sibling" to the left of the vertical
             * split line below.  The right side of the node contains
             * entries that will remain in the existing node (although
             * they've been shifted to the left).  The vertical bar (^)
             * indicates where the cursor currently points.
             *
             * case 1:
             *
             *   We need to set the cursor's "bin" reference to point
             *   at the new sibling, but we don't need to adjust its
             *   index since that continues to be correct post-split.
             *
             *   +=======================================+
             *   |  new sibling        |  existing node  |
             *   +=======================================+
             *         cursor ^
             *
             * case 2:
             *
             *   We only need to adjust the cursor's index since it
             *   continues to point to the current BIN post-split.
             *
             *   +=======================================+
             *   |  new sibling        |  existing node  |
             *   +=======================================+
             *                              cursor ^
             *
             * case 3:
             *
             *   Do nothing.  The cursor continues to point at the
             *   correct BIN and index.
             *
             *   +=======================================+
             *   |  existing Node        |  new sibling  |
             *   +=======================================+
             *         cursor ^
             *
             * case 4:
             *
             *   Adjust the "bin" pointer to point at the new sibling BIN
             *   and also adjust the index.
             *
             *   +=======================================+
             *   |  existing Node        |  new sibling  |
             *   +=======================================+
             *                                 cursor ^
             */
            BIN ns = (BIN) newSibling;
            if (newSiblingLow == 0) {
                if (cIdx < newSiblingHigh) {
                    /* case 1 */
                    setCursorBIN(cursor, ns);
                    iter.remove();
                    ns.addCursor(cursor);
                } else {
                    /* case 2 */
                    setCursorIndex(cursor, cIdx - adjustmentDelta);
                }
            } else {
                if (cIdx >= newSiblingLow) {
                    /* case 4 */
                    setCursorIndex(cursor, cIdx - newSiblingLow);
                    setCursorBIN(cursor, ns);
                    iter.remove();
                    ns.addCursor(cursor);
                }
            }
        }
    }

    /**
     * For each cursor in this BIN's cursor set, ensure that the
     * cursor is actually referring to this BIN.
     */
    public void verifyCursors() {
        if (cursorSet != null) {
            Iterator iter = cursorSet.iterator();
            while (iter.hasNext()) {
                CursorImpl cursor = (CursorImpl) iter.next();
                BIN cBin = getCursorBIN(cursor);
                assert cBin == this;
            }
        }
    }

    /**
     * Adjust cursors referring to this BIN following an insert.
     *
     * @param insertIndex - The index of the new entry.
     */
    void adjustCursorsForInsert(int insertIndex) {
        assert this.getLatch().isOwner();
        /* cursorSet may be null if this is being created through
           createFromLog() */
        if (cursorSet != null) {
            Iterator iter = cursorSet.iterator();
            while (iter.hasNext()) {
                CursorImpl cursor = (CursorImpl) iter.next();
                int cIdx = getCursorIndex(cursor);
                if (insertIndex <= cIdx) {
                    setCursorIndex(cursor, cIdx + 1);
                }
            }
        }
    }

    /**
     * Compress this BIN by removing any entries that are deleted.  Deleted
     * entries are those that have LN's marked deleted or if the knownDeleted
     * flag is set. Caller is responsible for latching and unlatching
     * this node.
     *
     * @param binRef is used to determine the set of keys to be checked for
     * deletedness, or is null to check all keys.
     *
     * @return true if there are no more entries in this BIN and it should
     * be removed from the parent, false if more entries remain.
     */
    public boolean compress(BINReference binRef)
        throws DatabaseException {

        boolean ret = false;
        boolean setNewIdKey = false;
        boolean anyLocksDenied = false;
	DatabaseImpl db = getDatabase();
        Locker lockingTxn = new BasicLocker(db.getDbEnvironment());

        try {
            for (int i = 0; i < getNEntries(); i++) {

		/* 
		 * We have to be able to lock the LN before we can
		 * compress the entry.  If we can't, then, skip over
		 * it. 
                 * We must lock the LN even if isKnownDeleted is
		 * true, because locks protect the aborts. (Aborts 
                 * may execute multiple operations, where each operation
                 * latches and unlatches. It's the LN lock that protects
                 * the integrity of the whole multi-step process.)
                 *
                 * For example, during abort, there may be cases where we
		 * have deleted and then added an LN during the same
		 * txn.  This means that to undo/abort it, we first
		 * delete the LN (leaving knownDeleted set), and then
		 * add it back into the tree.  We want to make sure
		 * the entry is in the BIN when we do the insert back
		 * in.
		 */
                boolean deleteEntry = false;
                ChildReference child = getEntry(i);
                if (binRef == null || binRef.hasDeletedKey(child.getKey())) {
                    Node n = child.fetchTargetIgnoreKnownDeleted(db, this);
                    if (n == null) {
                        /* Cleaner deleted the log file.  Compress this LN. */
                        deleteEntry = true;
                    } else if (child.isKnownDeleted()) {
                        LockGrantType lockRet =
                            lockingTxn.nonBlockingReadLock((LN) n);
                        if (lockRet == LockGrantType.DENIED) {
                            anyLocksDenied = true;
                            continue;
                        }

                        deleteEntry = true;
                    } else {
                        if (!n.containsDuplicates()) {
                            LN ln = (LN) n;
                            LockGrantType lockRet =
                                lockingTxn.nonBlockingReadLock(ln);
                            if (lockRet == LockGrantType.DENIED) {
                                anyLocksDenied = true;
                                continue;
                            }

                            if (ln.isDeleted()) {
                                deleteEntry = true;
                            }
                        }
                    }

                    /* Remove key from BINReference in case we requeue it. */
                    if (binRef != null) {
                        binRef.removeDeletedKey(child.getKey());
                    }
                }

                /* At this point, we know we can delete. */
                if (deleteEntry) {
                    Comparator userComparisonFcn = getKeyComparator();
                    boolean entryIsIdentifierKey =
                        (userComparisonFcn == null ?
                         child.getKey().compareTo(getIdentifierKey()) :
                         userComparisonFcn.compare
			 (child.getKey().getKey(),
			  getIdentifierKey().getKey()))
                        == 0;
                    if (entryIsIdentifierKey) {
                        /* 
                         * We're about to remove the entry with the idKey so
                         * the node will need a new idkey.
                         */
                        setNewIdKey = true;
                    }

                    boolean deleteSuccess = deleteEntry(i, true);
                    assert deleteSuccess;
                    /*
                     * Since we're deleting the current entry, bump the
                     * current index back down one.
                     */
                    i--;
                }
            }
        } finally {
            if (lockingTxn != null) {
                lockingTxn.operationEnd();
            }
        }

        if (anyLocksDenied && binRef != null) {
            db.getDbEnvironment().addToCompressorQueue(binRef);
        }

        if (getNEntries() == 0) {
            ret = true;
        } else {
            if (setNewIdKey) {
                setIdentifierKey(getEntry(0).getKey());
            }
        }

        return ret;
    }

    /**
     * Reduce memory consumption by evicting all LN targets. Note that the
     * targets are not persistent, so this doesn't affect node dirtiness.
     *
     * The BIN should be latched by the caller.
     * @return number of evicted bytes
     */
    public long evictLNs()
        throws DatabaseException {

        assert getLatch().isOwner() :
            "BIN must be latched before evicting LNs";

        /* We can't evict an LN which is pointed to by a cursor, in case that
         * cursor has a reference to the LN object. We'll take the cheap
         * choice and avoid evicting any LNs if there are cursors on this
         * BIN. We could do a more expensive, precise check to see entries 
         * have which cursors. (We'd have to be careful to use the right
         * field, index vs dupIndex). This is something we might move to
         * later.
         */
        long removed = 0;
        if (nCursors() == 0) {
            for (int i = 0; i < getNEntries(); i++) {
                ChildReference lnRef = getEntry(i);
                Node n = lnRef.getTarget();
                if (n != null) {
                    if (n instanceof LN) {
                        lnRef.setTarget(null);
                        removed += n.getMemorySizeIncludedByParent();
                    }
                }
            }
            updateMemorySize(removed, 0);
        }
        return removed;
    }

    /* For debugging.  Overrides method in IN. */
    boolean validateSubtreeBeforeDelete(int index)
        throws DatabaseException {

        return true;
    }

    /**
     * Check if this node fits the qualifications for being part of a deletable
     * subtree. It can only have one IN child and no LN children.
     */
    boolean isValidForDelete()
        throws DatabaseException {

        /* 
	 * Can only have one valid child, and that child should be deletable.
	 */
        int validIndex = 0;
        int numValidEntries = 0;
	boolean needToLatch = !getLatch().isOwner();
	try {
	    if (needToLatch) {
		latch();
	    }
	    for (int i = 0; i < getNEntries(); i++) {
		if (!getEntry(i).isKnownDeleted()) {
		    numValidEntries++;
		    validIndex = i;
		}
	    }

	    if (numValidEntries > 1) {      // more than 1 entry
		return false;
	    } else {
		if (nCursors() > 0) {        // cursors on BIN, not eligable
		    return false;
		} 
		if (numValidEntries == 1) {  // need to check child (DIN or LN)
		    Node child =
			getEntry(validIndex).fetchTarget(getDatabase(), this);
		    return child.isValidForDelete();
		} else {
		    return true;             // 0 entries.
		}
	    }
	} finally {
	    if (needToLatch &&
		getLatch().isOwner()) {
		releaseLatch();
	    }
	}
    }

    /*
     * DbStat support.
     */
    void accumulateStats(TreeWalkerStatsAccumulator acc) {
	acc.processBIN(this, new Long(getNodeId()), getLevel());
    }

    /**
     * Return the relevant user defined comparison function for this
     * type of node.  For IN's and BIN's, this is the BTree Comparison
     * function.  Overriden by DBIN.
     */
    public Comparator getKeyComparator() {
        return getDatabase().getBtreeComparator();
    }

    public String beginTag() {
        return BEGIN_TAG;
    }

    public String endTag() {
        return END_TAG;
    }

    /*
     * Logging support
     */

    /**
     * @see LoggableObject#getLogType
     */
    public LogEntryType getLogType() {
        return LogEntryType.LOG_BIN;
    }

    public String shortClassName() {
        return "BIN";
    }

    /**
     * Decide how to log this node. BINs may be logged provisionally. If
     * logging a delta, return an  null for the lsn.
     */
    protected DbLsn logInternal(LogManager logManager, 
                                boolean allowDeltas,
                                boolean isProvisional) 
        throws DatabaseException {

        boolean doDeltaLog = false;

        /* 
         * We can log a delta rather than full version of this BIN if 
         * - this has been called from the checkpointer with allowDeltas=true
         * - there is a full version on disk
         * - we meet the percentage heuristics defined by environment params.
         * - this delta is not prohibited because of cleaning or compression
         * All other logging should be of the full version.
         */
        BINDelta deltaInfo = null;
        if ((allowDeltas) &&
            (lastFullVersion != null) &&
            !prohibitNextDelta){
            deltaInfo = new BINDelta(this);
            doDeltaLog = doDeltaLog(deltaInfo);
        }

        DbLsn returnLsn = null;
        if (doDeltaLog) {
            /* 
	     * Don't change the dirtiness of the node -- leave it
	     * dirty. Deltas are never provisional, they must be
	     * processed at recovery time.
             */
            lastDeltaVersion = logManager.log(deltaInfo);
            returnLsn = null;
            numDeltasSinceLastFull++;
        } else {
            returnLsn = logManager.log(new INLogEntry(this), isProvisional,
                                       null, false);
            lastFullVersion = returnLsn;
            numDeltasSinceLastFull = 0;
            setDirty(false);
        }
        prohibitNextDelta = false;
        return returnLsn;
    }

    /** 
     * Decide whether to log a full or partial BIN, depending on the ratio of 
     * the delta size to full BIN size, and the number of deltas that
     * have been logged since the last full.
     * 
     * @return true if we should log the deltas of this BIN
     */
    private boolean doDeltaLog(BINDelta deltaInfo) 
        throws DatabaseException {

        int maxDiffs = (getNEntries() * 
                        getDatabase().getBinDeltaPercent())/100;
        if ((deltaInfo.getNumDeltas() <= maxDiffs) &&
            (numDeltasSinceLastFull < getDatabase().getBinMaxDeltas())) {
            return true;
        } else {
            return false;
        }
    }
}
