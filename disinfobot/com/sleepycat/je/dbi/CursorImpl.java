/*
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: CursorImpl.java,v 1.1 2004/11/22 18:27:59 kate Exp $
*/

package com.sleepycat.je.dbi;

import java.util.Comparator;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.LockStats;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.RunRecoveryException;
import com.sleepycat.je.latch.Latch;
import com.sleepycat.je.latch.LatchNotHeldException;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.ChildReference;
import com.sleepycat.je.tree.DBIN;
import com.sleepycat.je.tree.DIN;
import com.sleepycat.je.tree.DupCountLN;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.tree.Key;
import com.sleepycat.je.tree.LN;
import com.sleepycat.je.tree.Node;
import com.sleepycat.je.tree.Tree;
import com.sleepycat.je.tree.TreeWalkerStatsAccumulator;
import com.sleepycat.je.txn.BasicLocker;
import com.sleepycat.je.txn.LockGrantType;
import com.sleepycat.je.txn.LockResult;
import com.sleepycat.je.txn.Locker;
import com.sleepycat.je.txn.ThreadLocker;
import com.sleepycat.je.utilint.DbLsn;

/**
 * A CursorImpl is the internal implementation of the cursor.
 */
public class CursorImpl implements Cloneable {

    private static final boolean DEBUG = false;

    private static final byte CURSOR_NOT_INITIALIZED = 1;
    private static final byte CURSOR_INITIALIZED = 2;
    private static final byte CURSOR_CLOSED = 3;
    private static final String TRACE_DELETE = "delete";
    private static final String TRACE_MOD = "Mod:";

    /*
     * Cursor location in the database, represented by a BIN and an index in
     * the BIN.  bin/index must have a non-null/non-negative value if dupBin is
     * set to non-null.
     */
    volatile private BIN bin;
    volatile private int index;

    /*
     * Cursor location in a given duplicate set.  If the cursor is not
     * referencing a duplicate set then these are null.
     */
    volatile private DBIN dupBin;
    volatile private int dupIndex;

    /*
     * The cursor location used for a given operation. 
     */
    private BIN targetBin;
    private int targetIndex;
    private Key dupKey;

    /* The database behind the handle. */
    private DatabaseImpl database;

    /* Attributes */
    private boolean dirtyReadDefault;

    /* Owning transaction. */
    private Locker locker;

    /* Release locks every time the non-transactional cursor is closed. */
    private boolean releaseLocksOnClose;

    /* State of the cursor. See CURSOR_XXX above. */
    private byte status;

    private TreeWalkerStatsAccumulator treeStatsAccumulator;

    public void incrementLNCount() {
	if (treeStatsAccumulator != null) {
	    treeStatsAccumulator.incrementLNCount();
	}
    }

    /**
     * public for Cursor et al
     */
    static public class SearchMode {
        public static final SearchMode SET =        new SearchMode();
        public static final SearchMode BOTH =       new SearchMode();
        public static final SearchMode SET_RANGE =  new SearchMode();
        public static final SearchMode BOTH_RANGE = new SearchMode();
    }

    /**
     * Creates a cursor.  The retainNonTxnLocks parameter only has meaning when
     * the locker is a BasicLocker, i.e., when locker.isTransactional() returns
     * false.  In that case, if retainNonTxnLocks==true, locks will be held
     * until someone calls Locker.operationEnd explicitly.  If
     * retainNonTxnLocks==false, this class will call Locker.endOperation
     * internally whenever the cursor is closed and will create a new
     * transaction every time cloneCursor is called.
     */
    public CursorImpl(DatabaseImpl database,
		      Locker locker,
                      boolean retainNonTxnLocks,
		      CursorConfig cursorConfig)
        throws DatabaseException {

        init(database, locker, retainNonTxnLocks,
             (cursorConfig != null) ? cursorConfig.getDirtyRead() : false);
    }

    /**
     * Creates a cursor with retainNonTxnLocks=true and dirtyRead=false.
     */
    public CursorImpl(DatabaseImpl database, Locker locker) 
        throws DatabaseException {

        init(database, locker, true, false);
    }

    private void init(DatabaseImpl database,
		      Locker locker,
                      boolean retainNonTxnLocks,
		      boolean configDirtyRead)
        throws DatabaseException {

        bin = null;
        index = -1;
        dupBin = null;
        dupIndex = -1;

        // retainNonTxnLocks=true should not be used with a ThreadLocker
        assert !(retainNonTxnLocks && (locker instanceof ThreadLocker));

        // retainNonTxnLocks=false should not be used with a BasicLocker
        assert !(!retainNonTxnLocks && locker.getClass() == BasicLocker.class);

        releaseLocksOnClose = !retainNonTxnLocks && !locker.isTransactional();

        // Associate this cursor with the database
        this.database = database;
        this.locker = locker;
        this.locker.registerCursor(this);

        // Set dirty read attributes
        dirtyReadDefault = configDirtyRead || locker.isDirtyReadDefault();

        status = CURSOR_NOT_INITIALIZED;
    }

    /**
     * Shallow copy.  Caller is responsible for adding the cursor to the BIN.
     */
    public CursorImpl cloneCursor()
        throws DatabaseException {

        try {
            CursorImpl ret = (CursorImpl) super.clone();
            if (releaseLocksOnClose) {
                /* A new ThreadLocker must be created for each new cursor. */
                ret.locker = locker.newInstance();
            }
            ret.locker.registerCursor(ret);
            return ret;
        } catch (CloneNotSupportedException cannotOccur) {
	    return null;
	}
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int idx) {
        index = idx;
    }

    public BIN getBIN() {
        return bin;
    }

    public void setBIN(BIN newBin) {
        bin = newBin;
    }

    public int getDupIndex() {
        return dupIndex;
    }

    public void setDupIndex(int dupIdx) {
        dupIndex = dupIdx;
    }

    public DBIN getDupBIN() {
        return dupBin;
    }

    public void setDupBIN(DBIN newDupBin) {
        dupBin = newDupBin;
    }

    public void setTreeStatsAccumulator(TreeWalkerStatsAccumulator tSA) {
	treeStatsAccumulator = tSA;
    }

    /**
     * Figure out which bin/index set to use
     */
    private boolean setTargetBin() {
        targetBin = null;
        targetIndex = 0;
        boolean isDup = (dupBin != null);
        dupKey = null;
        if (isDup) {
            targetBin = dupBin;
            targetIndex = dupIndex;
            dupKey = dupBin.getDupKey();
        } else {
            targetBin = bin;
            targetIndex = index;
        }
        return isDup;
    }

    public void latchBIN()
        throws DatabaseException {

	while (bin != null) {
	    BIN waitingOn = bin;
	    waitingOn.latch();
	    if (bin == waitingOn) {
		return;
	    }
	    waitingOn.releaseLatch();
	}
    }

    public void releaseBIN()
        throws LatchNotHeldException {

        if ((bin != null) && (bin.getLatch().isOwner())){
            bin.releaseLatch();
        }
    }

    public void latchBINs()
        throws DatabaseException {

        latchBIN();
        latchDBIN();
    }

    public void releaseBINs()
        throws LatchNotHeldException {

        releaseBIN();
        releaseDBIN();
    }

    public void latchDBIN()
        throws DatabaseException {

	while (dupBin != null) {
	    BIN waitingOn = dupBin;
	    waitingOn.latch();
	    if (dupBin == waitingOn) {
		return;
	    }
	    waitingOn.releaseLatch();
	}
    }

    public void releaseDBIN()
        throws LatchNotHeldException {

        if ((dupBin != null) && (dupBin.getLatch().isOwner())){
            dupBin.releaseLatch();
        }
    }

    public Locker getLocker() {
        return locker;
    }

    public void addCursor(BIN bin) {
        if (bin != null) {
            assert bin.getLatch().isOwner();
            bin.addCursor(this);
        }
    }

    /**
     * Add to the current cursor. (For dups)
     */
    public void addCursor() {
        if (dupBin != null) {
            addCursor(dupBin);
        }
        if (bin != null) {
            addCursor(bin);
        }
    }

    /*
     * Update a cursor to refer to a new BIN or DBin following an insert.
     * Don't bother removing this cursor from the previous bin.  Cursor will do
     * that with a cursor swap thereby preventing latch deadlocks down here.
     */
    public void updateBin(BIN bin, int index) {
        setDupIndex(-1);
        setDupBIN(null);
        setIndex(index);
        setBIN(bin);
        addCursor(bin);
    }

    public void updateDBin(DBIN dupBin, int dupIndex) {
        setDupIndex(dupIndex);
        setDupBIN(dupBin);
        addCursor(dupBin);
    }

    private void removeCursor()
        throws DatabaseException {

        removeCursorBin(bin);
        removeCursorBin(dupBin);
    }

    private void removeCursorBin(BIN aBin)
        throws DatabaseException {

        if (aBin != null) {
            aBin.latch();
            aBin.removeCursor(this);
            aBin.releaseLatch();
        }
    }
    
    public void dumpTree()
        throws DatabaseException {

        database.getTree().dump();
    }

    /**
     * @return true if this cursor is closed
     */
    public boolean isClosed() {
        return (status == CURSOR_CLOSED);
    }

    /**
     * @return true if this cursor is not initialized
     */
    public boolean isNotInitialized() {
        return (status == CURSOR_NOT_INITIALIZED);
    }

    /**
     * Reset a cursor to an uninitialized state, but unlike close(), allow it
     * to be used further.
     */
    public void reset()
        throws DatabaseException {

        removeCursor();

        if (releaseLocksOnClose) {
            // Release locks for non-transactional operations.
            locker.operationEnd();
        }

        bin = null;
        index = -1;
        dupBin = null;
        dupIndex = -1;

        status = CURSOR_NOT_INITIALIZED;
    }

    /**
     * Close a cursor.
     * @throws DatabaseException if the cursor was previously closed.
     */
    public void close()
        throws DatabaseException {

        assert assertCursorState(false) : dumpToString(true);

        removeCursor();
        locker.unRegisterCursor(this);

        if (releaseLocksOnClose) {
            // Release locks for non-transactional operations.
            locker.operationEnd();
        }

        status = CURSOR_CLOSED;
    }

    public int count()
        throws DatabaseException {

        assert assertCursorState(true) : dumpToString(true);

        boolean duplicatesAllowed = database.getSortedDuplicates();
        if (!duplicatesAllowed) {
            return 1;
        }

        if (bin == null) {
            return 0;
        }

	latchBIN();

        if (bin.getNEntries() <= index) {
	    releaseBIN();
            return 0;
        }

        ChildReference entry = bin.getEntry(index);

        int dupRootCount = 1;

        Node n = entry.fetchTarget(database, bin);
        if (n.containsDuplicates()) {
            DIN dupRoot = (DIN) n;
            /* Latch couple down the tree. */
            dupRoot.latch();
	    releaseBIN();
            DupCountLN dupCountLN = (DupCountLN)
                dupRoot.getDupCountLNRef().fetchTarget(database, dupRoot);
            /* We can't hold latches when we acquire locks. */
            dupRoot.releaseLatch();
            locker.readLock(dupCountLN);
            dupRootCount = dupCountLN.getDupCount();
        } else {
	    releaseBIN();
        }

        return dupRootCount;
    }

    /**
     * Delete the item pointed to by the cursor. If cursor it not initialized
     * or item is already deleted, return appropriate codes. Returns with
     * nothing latched.  bin and dupBin are latched as appropriate.
     *
     * @return 0 on success, appropriate error code otherwise.
     */
    public OperationStatus delete()
        throws DatabaseException {

        assert assertCursorState(true) : dumpToString(true);
        boolean isDup = setTargetBin();

        /* If nothing at current position, return. */
        if (targetBin == null) {
            return OperationStatus.KEYEMPTY;
        }

        ChildReference entry = targetBin.getEntry(targetIndex);

        /* 
         * Check if this is already deleted. We may know that the record is
         * deleted w/out seeing the LN.
         */
        if (entry.isKnownDeleted()) {
            releaseBINs();
            return OperationStatus.KEYEMPTY;
        }

        /*
         * Release latches, lock LN. Don't access BIN until latched again.
         */
        LN ln = (LN) entry.fetchTarget(database, targetBin);
        releaseBINs();
        LockResult lockResult = locker.writeLock(ln, database);
        LockGrantType lockStatus = lockResult.getLockGrant();
            
        /* Check LN deleted status under the protection of a write lock. */
        if (ln.isDeleted()) {
            revertLock(locker, ln, lockStatus);
            return OperationStatus.KEYEMPTY;
        }

	DIN dupRoot = null;

        /*
         * Between the release of the BIN latch and acquiring the write lock
         * any number of operations may have executed which would result in a
         * new abort LSN for this record. Therefore, wait until now to get the
         * abort LSN.
         */
	if (isDup) {

	    /*
	     * We can't call latchBINs here because that would latch both bin
	     * and dbin.  Subsequently we'd have to latch the dupRoot which is
	     * higher in the tree than the dbin which would be latching up the
	     * tree.  So we have to latch in bin, dupRoot, dbin order.
	     */
	    latchBIN();
            /* Latch couple with the DupRoot. */
            dupRoot = (DIN) bin.getEntry(index).fetchTarget(database, bin);
            dupRoot.latch();

            latchDBIN();
	} else {
	    latchBINs();
	}
        setTargetBin();   // Cursor adjustments could have happened earlier.
        entry = targetBin.getEntry(targetIndex);
        DbLsn oldLsn = entry.getLsn();
        Key lnKey = entry.getKey();
        Key idKey = targetBin.getIdentifierKey();
        long nodeId = targetBin.getNodeId();
        lockResult.setAbortLsn(oldLsn, entry.isKnownDeleted());

        long oldLNSize = ln.getMemorySizeIncludedByParent();
        DbLsn newLsn = ln.delete(database, lnKey, dupKey, oldLsn, locker);
        long newLNSize = ln.getMemorySizeIncludedByParent();

        /*
         * Now update the parent of the LN (be it BIN or DBIN) to correctly
         * reference the LN and adjust the memory sizing.  Latch again and
         * reset BINs in case they changed during unlatched time. Be sure to do
         * this update of the LSN before updating the dup count LN. In case we
         * encounter problems there we need the LSN to match the latest version
         * to ensure that undo works.
         */
        if (isDup) {

            /* 
             * Save a reference to the affected parent of this LN to place on
             * the delete info queue when the txn commits. If this is a
             * duplicate tree, we also have to update the duplicate tree count.
             */
            targetBin.updateEntry(targetIndex, newLsn);
            targetBin.updateMemorySize(oldLNSize, newLNSize);
            releaseBINs();

            ChildReference dupCountRef = dupRoot.getDupCountLNRef();
            DupCountLN dcl = (DupCountLN)
                dupCountRef.fetchTarget(database, dupRoot);
            dupRoot.releaseLatch();
            LockResult dclGrantAndInfo = locker.writeLock(dcl, database);
            LockGrantType dclLockStatus = dclGrantAndInfo.getLockGrant();
 
            /*
             * The write lock request might have blocked while waiting for a
             * transaction that changed the oldLsn.  Re-get the reference to
             * the LN and get the old (abort) LSN out of it.
             */
            latchBIN();
            dupRoot = (DIN) bin.getEntry(index).fetchTarget(database, bin);
            dupRoot.latch();
            releaseBIN();
            dupCountRef = dupRoot.getDupCountLNRef();
	    assert dupCountRef.fetchTarget(database, dupRoot) == dcl;
            DbLsn oldDclLsn = dupCountRef.getLsn();
            dclGrantAndInfo.setAbortLsn(oldDclLsn,
                                        dupCountRef.isKnownDeleted());
            dcl.decDupCount();
	    assert dcl.getDupCount() >= 0;
            EnvironmentImpl envImpl = database.getDbEnvironment();
            DbLsn dupCountLsn =
                dcl.log(envImpl, database.getId(), dupKey, oldDclLsn, locker);
            dupRoot.updateDupCountLNRef(dupCountLsn);
            dupRoot.releaseLatch();

            locker.addDeleteInfo(dupBin, lnKey);
        } else {
            targetBin.updateEntry(targetIndex, newLsn);
            targetBin.updateMemorySize(oldLNSize, newLNSize);
            releaseBINs();

            locker.addDeleteInfo(bin, lnKey);
        } 

        trace(Level.FINER, TRACE_DELETE, targetBin,
              ln, targetIndex, oldLsn, newLsn);
            
        return OperationStatus.SUCCESS;
    } 

    /**
     * Return a new copy of the cursor. If position is true, position the
     * returned cursor at the same position.
     */
    public CursorImpl dup(boolean samePosition)
        throws DatabaseException {

        assert assertCursorState(false) : dumpToString(true);

        CursorImpl ret = cloneCursor();
        if (!samePosition) {
            ret.bin = null;
            ret.index = -1;
            ret.dupBin = null;
            ret.dupIndex = -1;

            ret.status = CURSOR_NOT_INITIALIZED;
        } else {
            latchBINs();
            ret.addCursor();
            releaseBINs();
        }

        return ret;
    }

    /*
     * Puts 
     */

    /**
     * Insert a new key/data pair in this tree.
     */
    public OperationStatus putLN(Key key, LN ln, boolean allowDuplicates)
        throws DatabaseException {

        assert assertCursorState(false) : dumpToString(true);

        assert Latch.countLatchesHeld() == 0;
	LockResult lockResult = null;
	lockResult = locker.writeLock(ln, database);
	LockGrantType lockStatus = lockResult.getLockGrant();

	/* 
	 * We'll set abortLsn down in Tree.insert when we know whether we're
	 * re-using a BIN entry or not.
	 */
        if (database.getTree().
            insert(ln, key, allowDuplicates, this, lockResult)) {
            status = CURSOR_INITIALIZED;
            return OperationStatus.SUCCESS;
        } else {
            locker.releaseLock(ln);
            return OperationStatus.KEYEXIST;
        }
    }

    /**
     * Insert or overwrite the key/data pair.
     * @param key
     * @param data
     * @return 0 if successful, XXX error value otherwise
     */
    public OperationStatus put(DatabaseEntry key,
			       DatabaseEntry data,
                               DatabaseEntry foundData) 
        throws DatabaseException {

        assert assertCursorState(false) : dumpToString(true);

        LN ln = new LN(data);
        OperationStatus result =
	    putLN(new Key(key), ln, database.getSortedDuplicates());
        if (result == OperationStatus.KEYEXIST) {
            status = CURSOR_INITIALIZED;

            /* 
             * If dups are allowed and putLN() returns KEYEXIST, the duplicate
             * already exists.  However, we still need to get a write lock, and
             * calling putCurrent does that.  Without duplicates, we have to
             * update the data of course.
             */
            result = putCurrent(data, null, foundData);
        }
        return result;
    }

    /**
     * Insert the key/data pair in No Overwrite mode.
     * @param key
     * @param data
     * @return 0 if successful, XXX error value otherwise
     */
    public OperationStatus putNoOverwrite(DatabaseEntry key,
					  DatabaseEntry data) 
        throws DatabaseException {

        assert assertCursorState(false) : dumpToString(true);

        LN ln = new LN(data);
        return putLN(new Key(key), ln, false);
    }

    /**
     * Insert the key/data pair as long as no entry for key/data exists yet.
     */
    public OperationStatus putNoDupData(DatabaseEntry key, DatabaseEntry data)
        throws DatabaseException {

        assert assertCursorState(false) : dumpToString(true);

        if (!database.getSortedDuplicates()) {
            throw new DatabaseException
                ("putNoDupData() called, but database is not configured " +
                 "for duplicate data.");
        }
        LN ln = new LN(data);
        return putLN(new Key(key), ln, true);
    }

    /**
     * Modify the current record with this data.
     * @param data
     */
    public OperationStatus putCurrent(DatabaseEntry data,
                                      DatabaseEntry foundKey,
                                      DatabaseEntry foundData)
        throws DatabaseException {

        assert assertCursorState(true) : dumpToString(true);

        if (foundKey != null) {
            foundKey.setData(null);
        }
        if (foundData != null) {
            foundData.setData(null);
        }

        if (bin == null) {
            return OperationStatus.KEYEMPTY;
        }
        
	latchBINs();
        boolean isDup = setTargetBin();

        try {

            /* 
             * Find the existing entry and get a reference to all BIN fields
             * while latched.
             */
            ChildReference entry = targetBin.getEntry(targetIndex);
            LN ln = (LN) entry.fetchTarget(database, targetBin);
            Key lnKey = entry.getKey();
            Comparator userComparisonFcn = targetBin.getKeyComparator();

            /* Unlatch the bin and get a write lock. */
            releaseBINs();

	    if (entry.isKnownDeleted() ||
		ln.isDeleted()) {
		return OperationStatus.NOTFOUND;
	    }

            LockResult lockResult = locker.writeLock(ln, database);
            LockGrantType lockStatus = lockResult.getLockGrant();

            /*
             * If cursor points at a dup, then we can only replace the entry
             * with a new entry that is "equal" to the old one.  Since a user
             * defined comparison function may actually compare equal for two
             * byte sequences that are actually different we still have to do
             * the replace.  Arguably we could skip the replacement if there is
             * no user defined comparison function and the new data is the
             * same.
             */
	    byte[] foundDataBytes;
	    byte[] foundKeyBytes;
	    if (isDup) {
		foundDataBytes = lnKey.getKey();
		foundKeyBytes = targetBin.getDupKey().getKey();
	    } else {
		foundDataBytes = ln.getData();
		foundKeyBytes = lnKey.getKey();
	    }
            byte[] newData;

            /* Resolve partial puts. */
            if (data.getPartial()) {
                int dlen = data.getPartialLength();
                int doff = data.getPartialOffset();
                int origlen = (foundDataBytes != null) ?
                                   foundDataBytes.length : 0;
                int oldlen = (doff + dlen > origlen) ? doff + dlen : origlen;
                int len = oldlen - dlen + data.getSize();
                newData = new byte[len];
                int pos = 0;

                // Keep 0..doff of the old data (truncating if doff > length).
                int slicelen = (doff < origlen) ? doff : origlen;
                if (slicelen > 0)
                    System.arraycopy(foundDataBytes, 0, newData,
				     pos, slicelen);
                pos += doff;

                // Copy in the new data.
                slicelen = data.getSize();
                System.arraycopy(data.getData(), data.getOffset(),
                                 newData, pos, slicelen);
                pos += slicelen;

                // Append the rest of the old data (if any).
                slicelen = origlen - (doff + dlen);
                if (slicelen > 0)
                    System.arraycopy(foundDataBytes, doff + dlen, newData, pos,
                                     slicelen);
            } else {
                int len = data.getSize();
                newData = new byte[len];
                System.arraycopy(data.getData(), data.getOffset(),
                                 newData, 0, len);
            }

            if (database.getSortedDuplicates()) {
		/* Check that data compares equal before replacing it. */
		boolean keysEqual = false;
		if (foundDataBytes != null) {
		    keysEqual = (userComparisonFcn == null ?
				 (Key.compareByteArray
                                  (foundDataBytes, newData) == 0) :
				 (userComparisonFcn.compare
                                  (foundDataBytes, newData) == 0));
		}
		if (!keysEqual) {
		    revertLock(locker, ln, lockStatus);
		    throw new DatabaseException
			("Can't replace a duplicate with different data.");
		}
	    }
	    if (foundData != null) {
		setDbt(foundData, foundDataBytes);
	    }
	    if (foundKey != null) {
		setDbt(foundKey, foundKeyBytes);
	    }

            /*
             * Between the release of the BIN latch and acquiring the write
             * lock any number of operations may have executed which would
             * result in a new abort lsn for this record. Therefore, wait until
             * now to get the abort lsn.
             */
            latchBINs();
            setTargetBin();
	    entry = targetBin.getEntry(targetIndex);
	    DbLsn oldLsn = entry.getLsn();
	    lockResult.setAbortLsn(oldLsn, entry.isKnownDeleted());

            /* 
	     * The modify has to be inside the latch so that the bin is updated
	     * inside the latch.
	     */
            long oldLNSize = ln.getMemorySizeIncludedByParent();
	    Key newKey = (isDup ? targetBin.getDupKey() : lnKey);
            DbLsn newLsn =
		ln.modify(newData, database, newKey, oldLsn, locker);
            long newLNSize = ln.getMemorySizeIncludedByParent();

            /* Update the parent BIN.*/
            targetBin.updateEntry(targetIndex, newLsn);
            targetBin.updateMemorySize(oldLNSize, newLNSize);
            releaseBINs();

            trace(Level.FINER, TRACE_MOD, targetBin,
                  ln, targetIndex, oldLsn, newLsn);

            status = CURSOR_INITIALIZED;
            return OperationStatus.SUCCESS;
        } finally {
            releaseBINs();
        }
    }

    /*
     * Gets 
     */

    /**
     * Retrieve the current record.
     */
    public OperationStatus getCurrent(DatabaseEntry foundKey,
				      DatabaseEntry foundData,
				      LockMode lockMode)
        throws DatabaseException {

        assert assertCursorState(true) : dumpToString(true);

        // If not pointing at valid entry, return failure
        if (bin == null) {
            return OperationStatus.KEYEMPTY;
        }

        if (dupBin == null) {
	    latchBIN();
        } else {
	    latchDBIN();
        }

        return getCurrentAlreadyLatched(foundKey, foundData, lockMode, true);
    }

    /**
     * Retrieve the current record. Assume the bin is already latched.  Return
     * with the target bin unlatched.
     */
    public OperationStatus getCurrentAlreadyLatched(DatabaseEntry foundKey,
						    DatabaseEntry foundData,
						    LockMode lockMode,
						    boolean first)
        throws DatabaseException {

        assert assertCursorState(true) : dumpToString(true);

        try {
            return fetchCurrent(foundKey, foundData, lockMode, first);
        } finally {
            releaseBINs();
        }
    }

    /**
     * Retrieve the current LN, return with the target bin unlatched.
     */
    LN getCurrentLN() 
        throws DatabaseException {

        assert assertCursorState(true) : dumpToString(true);

        if (bin == null) {
            return null;
        } else {
	    latchBIN();
            return getCurrentLNAlreadyLatched(LockMode.DEFAULT);
        }
    }


    /**
     * Retrieve the current LN, assuming the BIN is already latched.  Return
     * with the target BIN unlatched.
     */
    public LN getCurrentLNAlreadyLatched(LockMode lockMode) 
        throws DatabaseException {

        try {
            assert assertCursorState(true) : dumpToString(true);

            if (bin == null) {
                return null;
            }

            ChildReference entry = bin.getEntry(index);

            // Check the deleted flag in the bin.
            if (entry.isKnownDeleted()) {
		releaseBIN();
                return null;
            }

            // Get a reference to the LN under the latch.
            LN ln = (LN) entry.fetchTarget(database, bin);
            addCursor(bin);
        
            // Release latch, lock LN.
	    releaseBIN();
            LockResult lockResult = getReadLock(ln, lockMode);

            if (ln.isDeleted()) {
                revertLock(locker, ln, lockResult.getLockGrant());
                return null;
            } else {
		if (lockMode == LockMode.RMW) {
		    latchBIN();
		    entry = bin.getEntry(index);
		    DbLsn oldLsn = entry.getLsn();
		    lockResult.setAbortLsn(oldLsn, entry.isKnownDeleted());
		    releaseBIN();
		}
                return ln;
            }
        } finally {
            releaseBIN();
        }
    }

    /**
     * Move the cursor forward and return the next record. This will cross BIN
     * boundaries and dip into duplicate sets.
     *
     * @param foundKey DatabaseEntry to use for returning key
     *
     * @param foundData DatabaseEntry to use for returning data
     *
     * @param forward if true, move forward, else move backwards
     *
     * @param alreadyLatched if true, the bin that we're on is already
     * latched.
     *
     * @return 0 if successful, -1 if not.
     */
    public OperationStatus getNext(DatabaseEntry foundKey,
				   DatabaseEntry foundData,
				   LockMode lockMode,
				   boolean forward,
				   boolean alreadyLatched)
        throws DatabaseException {

        assert assertCursorState(true) : dumpToString(true);

        while (bin != null) {

            /* Are we positioned on a DBIN? */
            if (dupBin != null) {
                if (DEBUG) {
                    verifyCursor(dupBin);
                }
                if (getNextDuplicate(foundKey, foundData, lockMode,
                                     forward, alreadyLatched) ==
		    OperationStatus.SUCCESS) {
                    return OperationStatus.SUCCESS;
                } else {
                    removeCursorBin(dupBin);
                    alreadyLatched = false;
                    dupBin = null;
                    dupIndex = -1;
                    continue;
                }
            }

            if (!alreadyLatched) {
		latchBIN();
            } else {
                alreadyLatched = false;
            }

            if (DEBUG) {
                verifyCursor(bin);
            }           

            /* Is there anything left on this bin? */
            if ((forward && ++index < bin.getNEntries()) ||
                (!forward && --index > -1)) {

                OperationStatus ret =
		    getCurrentAlreadyLatched(foundKey, foundData,
					     lockMode, forward);
                if (ret == OperationStatus.SUCCESS) {
		    if (treeStatsAccumulator != null) {
			treeStatsAccumulator.incrementLNCount();
		    }
                    return OperationStatus.SUCCESS;
                } else {
                    continue;
                }

            } else {
                /* We need to go to the next bin. */
		releaseBIN();
                BIN newBin;

                if (forward) {
                    newBin = database.getTree().getNextBin(bin, null);
                } else {
                    newBin = database.getTree().getPrevBin(bin, null);
                }
                if (newBin == null) {
                    return OperationStatus.NOTFOUND;
                } else {
                    if (forward) {
                        index = -1;
                    } else {
                        index = newBin.getNEntries();
                    }
                    addCursor(newBin);
		    BIN oldBin = bin;
		    /* Ensure that setting bin is under newBin's latch */
                    bin = newBin;
                    newBin.releaseLatch();
                    removeCursorBin(oldBin);
                }
            }
        }
        return OperationStatus.NOTFOUND;
    }

    public OperationStatus getNextNoDup(DatabaseEntry foundKey,
					DatabaseEntry foundData, 
					LockMode lockMode,
					boolean forward)
        throws DatabaseException {

        assert assertCursorState(true) : dumpToString(true);

        if (dupBin != null) {
            removeCursorBin(dupBin);
            dupBin = null;
            dupIndex = -1;
        }

        return getNext(foundKey, foundData, lockMode, forward, false);
    }

    /** 
     * Enter with dupBin unlatched.  Pass foundKey == null to just advance
     * cursor to next duplicate without fetching data.
     */
    public OperationStatus getNextDuplicate(DatabaseEntry foundKey,
					    DatabaseEntry foundData, 
					    LockMode lockMode,
					    boolean forward,
					    boolean alreadyLatched)
        throws DatabaseException {

        assert assertCursorState(true) : dumpToString(true);

        while (dupBin != null) {
            if (!alreadyLatched) {
		latchDBIN();
            } else {
                alreadyLatched = false;
            }

            if (DEBUG) {
                verifyCursor(dupBin);
            }

            /* Are we still on this dbin? */
            if ((forward && ++dupIndex < dupBin.getNEntries()) ||
                (!forward && --dupIndex > -1)) {

                OperationStatus ret = OperationStatus.SUCCESS;
                if (foundKey != null) {
                    ret = fetchCurrent(foundKey, foundData, lockMode, forward);
                } else {
		    releaseDBIN();
                }
                if (ret == OperationStatus.SUCCESS) {
		    if (treeStatsAccumulator != null) {
			treeStatsAccumulator.incrementLNCount();
		    }
                    return ret;
                } else {
                    continue;
                }

            } else {
                /* We need to go to the next dupBin. */
		releaseDBIN();
                
		latchBIN();
                if (index < 0) {
                    /* This duplicate tree has been deleted. */
		    releaseBIN();
                    return OperationStatus.NOTFOUND;
                }
                DIN duplicateRoot = (DIN)
                    bin.getEntry(index).fetchTarget(database, bin);
		if (treeStatsAccumulator != null) {
		    DupCountLN dcl = duplicateRoot.getDupCountLN();
		    if (dcl != null) {
			dcl.accumulateStats(treeStatsAccumulator);
		    }
		}
		
		releaseBIN();
                assert (Latch.countLatchesHeld() == 0);

                DBIN newDupBin;
                if (forward) {
                    newDupBin = (DBIN) database.getTree().getNextBin
                        (dupBin, duplicateRoot);
                } else {
                    newDupBin = (DBIN) database.getTree().getPrevBin
                        (dupBin, duplicateRoot);
                }

                if (newDupBin == null) {
                    return OperationStatus.NOTFOUND;
                } else {
                    if (forward) {
                        dupIndex = -1;
                    } else {
                        dupIndex = newDupBin.getNEntries();
                    }
                    addCursor(newDupBin);
		    DBIN oldDupBin = dupBin;
		    /* Ensure that setting dupBin is under newDupBin's latch */
		    dupBin = newDupBin;
		    releaseDBIN();
                    removeCursorBin(oldDupBin);
                    assert(Latch.countLatchesHeld() == 0);
                }
            }
        }

        return OperationStatus.NOTFOUND;
    }

    /**
     * Position the cursor at the first or last record of the database.  It's
     * okay if this record is deleted. Returns with the target bin latched.
     * 
     * @return true if a first or last position is found, false if the 
     * tree being searched is empty.
     */
    public boolean positionFirstOrLast(boolean first, DIN duplicateRoot)
        throws DatabaseException {

        assert assertCursorState(false) : dumpToString(true);

        IN in = null;
        boolean found = false;
        try {
            if (duplicateRoot == null) {
                removeCursorBin(bin);
                if (first) {
                    in = database.getTree().getFirstNode();
                } else {
                    in = database.getTree().getLastNode();
                }

                if (in != null) {
                    if (in.getNEntries() > 0) {
                        if (in instanceof BIN) {
                            dupBin = null;
                            dupIndex = -1;
                            bin = (BIN) in;
                            index = (first ? 0 : (bin.getNEntries() - 1));
                            addCursor(bin);
                        } else {
                            throw new DatabaseException
                                ("getFirst/LastNode didn't return a BIN");
                        }

			ChildReference ref = in.getEntry(index);
                        if (ref.isKnownDeleted()) {

                            /* 
                             * If the entry is deleted, just leave our position
                             * here and return.
                             */
			    if (treeStatsAccumulator != null) {
				treeStatsAccumulator.incrementDeletedLNCount();
			    }
                            found = true;
                        } else {

                            /* 
			     * The entry is not deleted, so see if we need to
                             * descend further.
                             */
                            Node n = ref.fetchTarget(database, in);
                            if (n.containsDuplicates()) {
                                DIN dupRoot = (DIN) n;
                                dupRoot.latch();
                                in.releaseLatch();
                                in = null;
                                found = positionFirstOrLast(first, dupRoot);
                            } else {
				if (treeStatsAccumulator != null) {
				    if (((LN) n).isDeleted()) {
					treeStatsAccumulator.
					    incrementDeletedLNCount();
				    } else {
					treeStatsAccumulator.
					    incrementLNCount();
				    }
				}
                                found = true;
                            }
                        }
                    } else {
                        in.releaseLatch();
                    }
                }
            } else {
                removeCursorBin(dupBin);
                if (first) {
                    in = database.getTree().getFirstNode(duplicateRoot);
                } else {
                    in = database.getTree().getLastNode(duplicateRoot);
                }

                if (in != null) {

		    /* 
		     * An IN was found. Even if it's empty, let Cursor handle
		     * moving to the first non-deleted entry.
		     */
		    assert (in instanceof DBIN);

		    dupBin = (DBIN) in;
		    dupIndex = (first ? 0 : (dupBin.getNEntries() - 1));
		    addCursor(dupBin);
		    found = true;
                }
            }
            status = CURSOR_INITIALIZED;
            return found;
        } catch (DatabaseException e) {
            /* Release latch on error. */
            if (in != null) {
                in.releaseLatch();
            }
            throw e;
        }
    }

    public static final int FOUND = 0x1;
    /* EXACT match on the key portion. */
    public static final int EXACT = 0x2;
    /* EXACT match on the DATA portion when searchAndPositionBoth used. */
    public static final int EXACT_DATA = 0x4;

    /**
     * Position the cursor at the key. This returns a two part value that's
     * bitwise or'ed into the int. We find out if there was any kind of match
     * and if the match was exact. Note that this match focuses on whether the
     * searching criteria (key, or key and data, depending on the search type)
     * is met. The caller of this method will actually search down and find the
     * LN in question, and will be the one responsible for iterating down the
     * tree if there are deleted LNs.  Note this returns with the bin latched!
     */
    public int searchAndPosition(DatabaseEntry matchKey,
				 DatabaseEntry matchData,
				 SearchMode searchMode,
				 LockMode lockMode)
        throws DatabaseException {

        assert assertCursorState(false) : dumpToString(true);

        removeCursorBin(bin);
        bin = null;
        boolean foundSomething = false;
        boolean foundExact = false;
        boolean foundExactData = false;
        boolean exactSearch = (searchMode == SearchMode.SET ||
                               searchMode == SearchMode.BOTH);
        try {
            Key key = new Key(matchKey);
            bin = (BIN) database.getTree().
		search(key, Tree.SearchType.NORMAL, -1);

            if (bin != null) {
                addCursor(bin);

		/*
                 * If we're doing an exact search, tell bin.findEntry we
                 * require an exact match. If it's a range search, we don't
                 * need that exact match.
		 */
                index = bin.findEntry(key, true, exactSearch);

		/* 
		 * If we're doing an exact search, as a starting point, we'll *
		 * assume that we haven't found anything. If this is a range *
		 * search, we'll assume the opposite, that we have found * a
		 * record. That's because for a range search, the * higher
		 * level will take care of sorting out whether * anything is
		 * really there or not.
		 */
		foundSomething = !exactSearch;
		dupBin = null;
		dupIndex = -1;

                if (index >= 0) {
		    if ((index & IN.EXACT_MATCH) != 0) {

                        /* 
                         * The binary search told us we had an exact match.
                         * Note that this really only tells us that the key
                         * matched. The underlying LN may be deleted or the
                         * reference may be knownDeleted, or maybe there's a
                         * dup tree w/no entries, but the next layer up will
                         * find these cases.
                         */
			foundExact = true;
		    }
		    /* 
                     * Now turn off the exact match bit so the index will be a
                     * valid value, before we use it to retrieve the child
                     * reference from the bin.
                     */
		    index &= ~IN.EXACT_MATCH;
                    ChildReference ref = bin.getEntry(index);

                    if (!ref.isKnownDeleted()) {
                        Node n = ref.fetchTarget(database, bin);
                        boolean containsDuplicates = n.containsDuplicates();
                        if (searchMode == SearchMode.BOTH ||
                            searchMode == SearchMode.BOTH_RANGE) {

                            /* 
                             * We're not done with the search, this mode
                             * requires that we also match against the data
                             * field.
                             */
			    int searchResult =
				searchAndPositionBoth(containsDuplicates, n,
						      matchData, exactSearch,
						      lockMode, ref);

			    foundSomething = (searchResult & FOUND) != 0;

			    /* 
			     * Pass back as EXACT_DATA since we're matching
			     * on both key and data.
			     */
			    foundExactData = (searchResult & EXACT) != 0;
                        } else {
                            foundSomething = true;
                            if (!containsDuplicates && exactSearch) {
				/* Release latch, lock LN, check if deleted. */
				releaseBIN();
				LN ln = (LN) n;
				DbLsn oldLsn = ref.getLsn();
				LockResult lockResult =
				    getReadLock(ln, lockMode);
				latchBIN();

				if (ln.isDeleted()) {
				    foundSomething = false;
				    revertLock(locker, ln,
					       lockResult.getLockGrant());
				}

				/*
				 * LSN of LN could have changed while the bin
				 * was unlatched (i.e. lock request blocked for
				 * a writer).  Get the old LSN if necessary and
				 * write it into the lock info.
				 */
				ref = bin.getEntry(index);
				lockResult.setAbortLsn(ref.getLsn(),
						       ref.isKnownDeleted());
                            }
                        }
		    }
		}
            }
            status = CURSOR_INITIALIZED;

            /* Return a two part status value */
            return (foundSomething ? FOUND : 0) |
		(foundExact ? EXACT : 0) |
		(foundExactData ? EXACT_DATA : 0);
        } catch (DatabaseException e) {
            /* Release latch on error. */
	    releaseBIN();
            throw e;
        }
    }

    /**
     * For this type of search, we need to match both key and data. Do the data
     * portion of the match. We may be matching just against an LN, or doing
     * further searching into the dup tree.
     */
    private int searchAndPositionBoth(boolean containsDuplicates,
				      Node n,
				      DatabaseEntry matchData,
				      boolean exactSearch,
				      LockMode lockMode,
				      ChildReference ref)

        throws DatabaseException {

        assert assertCursorState(false) : dumpToString(true);

	boolean found = false;
	boolean exact = false;
	Key data = new Key(matchData);

	if (matchData == null) {
	    throw new IllegalArgumentException
		("null data passed to Tree.search().");
	}

        if (containsDuplicates) {
            /* It's a duplicate tree. */
            DIN duplicateRoot = (DIN) n;
            duplicateRoot.latch();
	    releaseBIN();
            dupBin = (DBIN) database.getTree().searchSubTree
                (duplicateRoot, data, Tree.SearchType.NORMAL, -1);
            if (dupBin != null) {
                /* Find an exact match. */
                addCursor(dupBin);
                dupIndex = dupBin.findEntry(data, true, exactSearch);
                if (dupIndex >= 0) {
		    if ((dupIndex & IN.EXACT_MATCH) != 0) {
			exact = true;
		    }
		    dupIndex &= ~IN.EXACT_MATCH;
                    found = true;
                } else {
                    dupIndex = -1;
                    found = !exactSearch;
                }
            }
        } else {
	    /* Not a duplicate, but checking for both key and data match. */
            LN ln = (LN) n;

	    /* Release latch, lock LN, check if deleted. */
	    releaseBIN();
	    DbLsn oldLsn = ref.getLsn();
	    LockResult lockResult = getReadLock(ln, lockMode);
	    latchBIN();

	    if (ln.isDeleted()) {
		found = false;
		revertLock(locker, ln, lockResult.getLockGrant());
	    } else {

		/*
		 * LSN of LN could have changed while the bin was unlatched
		 * (i.e. lock request blocked for a writer).  Get the old LSN
		 * if necessary and write it into the lock info.
		 */
		ref = bin.getEntry(index);
		lockResult.setAbortLsn(ref.getLsn(), ref.isKnownDeleted());
		dupBin = null;
		dupIndex = -1;
		int cmp = Key.compareByteArray(ln.getData(), data.getKey());
		found = (exactSearch ? (cmp == 0) : (cmp >= 0));
		exact = (cmp == 0);
	    }
        }

	return (found ? FOUND : 0) |
	    (exact ? EXACT : 0);
    }

    /* 
     * Lock and copy current record into the key and data DatabaseEntry. Enter
     * with the BIN/DBIN latched.
     */
    private OperationStatus fetchCurrent(DatabaseEntry foundKey,
					 DatabaseEntry foundData,
					 LockMode lockMode,
					 boolean first) 
        throws DatabaseException {

        boolean duplicateFetch = setTargetBin();
        if (targetBin == null) {
            return OperationStatus.NOTFOUND;
        }

        ChildReference entry = targetBin.getEntry(targetIndex);

        /* Check the deleted flag in the bin. */
        if (entry == null || entry.isKnownDeleted()) {
	    if (targetBin.getLatch().isOwner()) {
		targetBin.releaseLatch();
	    }
	    if (treeStatsAccumulator != null) {
		treeStatsAccumulator.incrementDeletedLNCount();
	    }
            return OperationStatus.KEYEMPTY;
        }

        /*
         * Note that since we have the BIN/DBIN latched, we can safely check
         * the node type. Any conversions from an LN to a dup tree must have
         * the bin latched.
         */
        addCursor(targetBin);
        Node n = entry.fetchTarget(database, targetBin);
        if (n.containsDuplicates()) {
            assert !duplicateFetch;
            /* Descend down duplicate tree, doing latch coupling. */
            DIN duplicateRoot = (DIN) n;
            duplicateRoot.latch();
            targetBin.releaseLatch();
            if (positionFirstOrLast(first, duplicateRoot)) {
                return fetchCurrent(foundKey, foundData, lockMode, first);
            } else {
                return OperationStatus.NOTFOUND;
            }
        }

        LN ln = (LN) n;
        /* Release latch, lock LN. */
        releaseBINs();

        LockResult lockResult = getReadLock(ln, lockMode);

	/*
	 * During the time that the latch was released, the cursor may have
	 * changed so we have to re-get the entry and check it for deletedness.
	 *
	 * During the lock acquisition, the ln may have changed because (e.g.)
	 * a delete may have been aborted in which case the LN we have in hand
	 * is no longer valid (but the lock is).  If it differs, reacquire the
	 * LN.
	 */
	latchBINs();
	try {
	    duplicateFetch = setTargetBin();
	    entry = targetBin.getEntry(targetIndex);

	    /* Check the deleted flag in the bin. */
	    if ((entry == null) || (entry.isKnownDeleted())) {
		targetBin.releaseLatch();   
		if (treeStatsAccumulator != null) {
		    treeStatsAccumulator.incrementDeletedLNCount();
		}
		return OperationStatus.KEYEMPTY;
	    }

	    DbLsn oldLsn = entry.getLsn();
	    if (lockMode == LockMode.RMW) {
		lockResult.setAbortLsn(oldLsn, entry.isKnownDeleted());
	    }

	    if (!entry.getLsn().equals(oldLsn) ||
		entry.getTarget() != n) {
		ln = (LN) entry.fetchTarget(database, targetBin);
	    }
	    if (ln.isDeleted()) {
		revertLock(locker, ln, lockResult.getLockGrant());
		if (treeStatsAccumulator != null) {
		    treeStatsAccumulator.incrementDeletedLNCount();
		}
		return OperationStatus.KEYEMPTY;
	    } else {
		if (duplicateFetch) {
		    if (foundData != null) {
			setDbt(foundData, entry.getKey().getKey());
		    }
		    if (foundKey != null) {
			setDbt(foundKey, targetBin.getDupKey().getKey());
		    }
		} else {
		    if (foundData != null) {
			setDbt(foundData, ln.getData());
		    }
		    if (foundKey != null) {
			setDbt(foundKey, entry.getKey().getKey());
		    }
		}

		return OperationStatus.SUCCESS;
	    }
	} finally {
	    releaseBINs();
	}
    }

    /**
     * Helper to return a Data DBT from a bin.
     */
    private void setDbt(DatabaseEntry data, byte[] bytes)
        throws DatabaseException {

        if (bytes != null) {
	    boolean partial = data.getPartial();
            int off = partial ? data.getPartialOffset() : 0;
            int len = partial ? data.getPartialLength() : bytes.length;
	    if (off + len > bytes.length)
		len = (off > bytes.length) ? 0 : bytes.length  - off;
            byte[] newdata = new byte[len];
            System.arraycopy(bytes, off, newdata, 0, len);
            data.setData(newdata);
            data.setOffset(0);
            data.setSize(len);
        } else {
            data.setData(null);
            data.setOffset(0);
            data.setSize(0);
        }

    }

    /*
     * For debugging. Verify that a BINs cursor set refers to the BIN.
     */
    private void verifyCursor(BIN bin)
        throws DatabaseException {

        if (!bin.getCursorSet().contains(this)) {
            throw new DatabaseException("BIN cursorSet is inconsistent.");
        }
    }

    /**
     * Calls checkCursorState and returns false is an exception is thrown.
     */
    private boolean assertCursorState(boolean mustBeInitialized) {
        try {
            checkCursorState(mustBeInitialized);
            return true;
        } catch (DatabaseException e) {
            return false;
        }
    }

    /**
     * Check that the cursor is open and optionally if it is initialized.
     */
    public void checkCursorState(boolean mustBeInitialized)
        throws DatabaseException {

        if (status == CURSOR_INITIALIZED) {

            if (DEBUG) {
                if (bin != null) {
                    verifyCursor(bin);
                }
                if (dupBin != null) {
                    verifyCursor(dupBin);
                }
            }           

            return;
        } else if (status == CURSOR_NOT_INITIALIZED) {
            if (mustBeInitialized) {
                throw new DatabaseException
                    ("Cursor Not Initialized.");
            }
        } else if (status == CURSOR_CLOSED) {
            throw new DatabaseException
                ("Cursor has been closed.");
        } else {
            throw new DatabaseException
                ("Unknown cursor status: " + status);
        }
    }

    /**
     * Figure out what kind of lock to get for this operation depending on
     * defaults and lock mode.
     */
    private LockResult getReadLock(LN ln, LockMode lockMode)
        throws DatabaseException {

        if (lockMode == null || lockMode == LockMode.DEFAULT) {
            if (!dirtyReadDefault) {
                LockGrantType lockGrant = locker.readLock(ln);
		return new LockResult(lockGrant, null);
            }
        } else if (lockMode == LockMode.RMW) {
	    return locker.writeLock(ln, database);
        }

        /* This is a dirty read, no need for a lock. */
	return new LockResult(LockGrantType.NONE_NEEDED, null);
    }

    /**
     * Returns whether the given lock mode will cause a dirty read when used
     * with this cursor.
     */
    public boolean isDirtyReadMode(LockMode lockMode) {
        
        /* Mimic the logic in getReadLock. */
        if (lockMode == null || lockMode == LockMode.DEFAULT) {
            return dirtyReadDefault;
        } else {
            return lockMode != LockMode.RMW;
        }
    }

    /**
     * Return this lock to its prior status. If the lock was just obtained,
     * release it. If it was promoted, demote it.
     */
    private void revertLock(Locker locker, LN ln, LockGrantType lockStatus) 
        throws DatabaseException {

        if ((lockStatus == LockGrantType.NEW) ||
            (lockStatus == LockGrantType.WAIT_NEW)) {
            locker.releaseLock(ln);
        } else if ((lockStatus == LockGrantType.PROMOTION) ||
                   (lockStatus == LockGrantType.WAIT_PROMOTION)){
            locker.demoteLock(ln.getNodeId());
        }
    }

    /**
     * @throws RunRecoveryException if the underlying environment is invalid.
     */
    public void checkEnv() 
        throws RunRecoveryException {
        
        database.getDbEnvironment().checkIfInvalid();
    }

    /**
     * Dump the cursor for debugging purposes.  Dump the bin and dbin that the
     * cursor refers to if verbose is true.
     */
    public void dump(boolean verbose) {
        System.out.println(dumpToString(verbose));
    }

    /**
     * dump the cursor for debugging purposes.  
     */
    public void dump() {
        System.out.println(dumpToString(true));
    }

    /* 
     * dumper
     */
    private String statusToString(byte status) {
        switch(status) {
        case CURSOR_NOT_INITIALIZED:
            return "CURSOR_NOT_INITIALIZED";
        case CURSOR_INITIALIZED:
            return "CURSOR_INITIALIZED";
        case CURSOR_CLOSED:
            return "CURSOR_CLOSED";
        default:
            return "UNKNOWN (" + Byte.toString(status) + ")";
        }
    }

    /* 
     * dumper
     */
    public String dumpToString(boolean verbose) {
        StringBuffer sb = new StringBuffer();

        sb.append("<Cursor idx=\"").append(index).append("\"");
        if (dupBin != null) {
            sb.append(" dupIdx=\"").append(dupIndex).append("\"");
        }
        sb.append(" status=\"").append(statusToString(status)).append("\"");
        sb.append(">\n");
	if (verbose) {
	    sb.append((bin == null) ? "" : bin.dumpString(2, true));
	    sb.append((dupBin == null) ? "" : dupBin.dumpString(2, true));
	}
        sb.append("\n</Cursor>");

        return sb.toString();
    }

    /*
     * For unit tests
     */
    public LockStats getLockStats() 
        throws DatabaseException {

        return locker.collectStats(new LockStats());
    }

    /**
     * Send trace messages to the java.util.logger. Don't rely on the logger
     * alone to conditionalize whether we send this message, we don't even want
     * to construct the message if the level is not enabled.
     */
    private void trace(Level level,
                       String changeType,
                       BIN theBin,
                       LN ln,
                       int lnIndex,
                       DbLsn oldLsn,
                       DbLsn newLsn) {
        Logger logger = database.getDbEnvironment().getLogger();
        if (logger.isLoggable(level)) {
            StringBuffer sb = new StringBuffer();
            sb.append(changeType);
            sb.append(" bin=");
            sb.append(theBin.getNodeId());
            sb.append(" ln=");
            sb.append(ln.getNodeId());
            sb.append(" lnIdx=");
            sb.append(lnIndex);
            sb.append(" oldLnLsn=");
            sb.append(oldLsn.getNoFormatString());
            sb.append(" newLnLsn=");
            sb.append(newLsn.getNoFormatString());
	
            logger.log(level, sb.toString());
        }
    }
}
