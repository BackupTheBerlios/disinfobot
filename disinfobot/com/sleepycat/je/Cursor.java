/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: Cursor.java,v 1.1 2004/11/22 18:27:59 kate Exp $
*/

package com.sleepycat.je;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.dbi.CursorImpl;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.GetMode;
import com.sleepycat.je.dbi.PutMode;
import com.sleepycat.je.dbi.CursorImpl.SearchMode;
import com.sleepycat.je.latch.Latch;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.ChildReference;
import com.sleepycat.je.tree.DBIN;
import com.sleepycat.je.tree.LN;
import com.sleepycat.je.tree.Node;
import com.sleepycat.je.txn.Locker;
import com.sleepycat.je.utilint.InternalException;

/**
 * Javadoc for this public class is generated
 * via the doc templates in the doc_src directory.
 */
public class Cursor {

    /**
     * The underlying cursor.
     */
    CursorImpl cursorImpl; // Used by subclasses.

    /**
     * The CursorConfig used to configure this cursor.
     */
    CursorConfig config;

    /**
     * True if update operations are prohibited through this cursor.  Update
     * operations are prohibited if the database is read-only or:
     *
     * (1) The database is transactional,
     *
     * and
     *
     * (2) The user did not supply a txn to the cursor ctor (meaning, the
     * locker is non-transactional).
     */
    private boolean updateOperationsProhibited;

    /**
     * Handle under which this cursor was created; may be null.
     */
    private Database dbHandle;

    /**
     * Database implementation.
     */
    private DatabaseImpl dbImpl;

    /**
     * Cursor constructor for an external transaction.
     * Not public. To get a cursor, the user should call Database.cursor();
     * If txn is null, a non-transactional cursor will be created that
     * releases locks for the prior operation when the next operation suceeds.
     */
    Cursor(Database dbHandle, Transaction txn, CursorConfig cursorConfig) 
        throws DatabaseException {

        this(dbHandle, (txn != null) ? txn.getLocker() : null, cursorConfig);
    }

    /**
     * Cursor constructor for an internal transaction.
     * Not public. To get a cursor, the user should call Database.cursor();
     * If locker is null or is non-transactional, a non-transactional cursor
     * will be created that releases locks for the prior operation when the
     * next operation suceeds.
     */
    Cursor(Database dbHandle, Locker locker, CursorConfig cursorConfig) 
        throws DatabaseException {

        if (locker != null && !locker.isTransactional()) {
            /* 
	     * Don't reuse a non-transactional locker, since we will not retain
             * locks and we will therefore release the locks held by the locker
             * given when the first operation occurs.  This could
             * unintentionally release locks for another cursor.
	     */
            locker = null;
        }
        locker = DatabaseUtil.getReadableLocker(dbHandle.getEnvironment(),
                                                dbHandle,
                                                locker,
                                                false /*retainNonTxnLocks*/);

        init(dbHandle, dbHandle.getDatabaseImpl(), locker,
             dbHandle.isWritable(), cursorConfig);
    }

    /**
     * Cursor constructor for an internal transaction with no db handle.  The
     * locker parameter must be non-null.
     */
    Cursor(DatabaseImpl dbImpl, Locker locker, CursorConfig cursorConfig) 
        throws DatabaseException {

        init(null, dbImpl, locker, true, cursorConfig);
    }

    private void init(Database dbHandle,
		      DatabaseImpl dbImpl,
                      Locker locker,
		      boolean isWritable,
                      CursorConfig cursorConfig) 
        throws DatabaseException {

        if (cursorConfig == null) {
            cursorConfig = CursorConfig.DEFAULT;
        }

        cursorImpl = new CursorImpl(dbImpl,
                                    locker,
                                    false /*retainNonTxnLocks*/,
                                    cursorConfig);

        updateOperationsProhibited =
            (dbImpl.isTransactional() && !locker.isTransactional()) ||
            !isWritable;
        this.dbImpl = dbImpl;
        this.dbHandle = dbHandle;
        if (dbHandle != null) {
            dbHandle.addCursor(this);
        }
	this.config = cursorConfig;
    }

    /**
     * Copy constructor.
     */
    Cursor(Cursor cursor, boolean samePosition)
        throws DatabaseException {

        cursorImpl = cursor.cursorImpl.dup(samePosition);
        dbImpl = cursor.dbImpl;
        dbHandle = cursor.dbHandle;
        if (dbHandle != null) {
            dbHandle.addCursor(this);
        }
        config = cursor.config;
    }

    /**
     * Internal entrypoint.
     */
    CursorImpl getCursorImpl() {
        return cursorImpl;
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public Database getDatabase() {
	return dbHandle;
    }

    /**
     * Always returns non-null, while getDatabase() returns null if no handle
     * is associated with this cursor.
     */
    DatabaseImpl getDatabaseImpl() {
	return dbImpl;
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public CursorConfig getConfig() {
	return config.cloneConfig();
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public synchronized void close()
        throws DatabaseException {

        checkState(false);
        cursorImpl.close();
        if (dbHandle != null) {
            dbHandle.removeCursor(this);
        }
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public int count()
        throws DatabaseException {
        
        checkState(true);
        trace(Level.FINEST, "Cursor.count: ", null);
        return countInternal();
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public Cursor dup(boolean samePosition)
        throws DatabaseException {

        checkState(false);
        return new Cursor(this, samePosition);
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public OperationStatus delete()
        throws DatabaseException {

        checkState(true);
        checkUpdatesAllowed("delete");
        trace(Level.FINEST, "Cursor.delete: ", null);

        return deleteInternal();
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public OperationStatus put(DatabaseEntry key, DatabaseEntry data) 
        throws DatabaseException {

        checkState(false);
        DatabaseUtil.checkForNullDbt(key, "key", true);
        DatabaseUtil.checkForNullDbt(data, "data", true);
        DatabaseUtil.checkForPartialKey(key);
        checkUpdatesAllowed("put");
        trace(Level.FINEST, "Cursor.put: ", key, data, null);

        return putInternal(key, data, PutMode.OVERWRITE);
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public OperationStatus putNoOverwrite(DatabaseEntry key,
                                          DatabaseEntry data) 
        throws DatabaseException {

        checkState(false);
        DatabaseUtil.checkForNullDbt(key, "key", true);
        DatabaseUtil.checkForNullDbt(data, "data", true);
        DatabaseUtil.checkForPartialKey(key);
        checkUpdatesAllowed("putNoOverwrite");
        trace(Level.FINEST, "Cursor.putNoOverwrite: ", key, data, null);

        return putInternal(key, data, PutMode.NOOVERWRITE);
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public OperationStatus putNoDupData(DatabaseEntry key, DatabaseEntry data) 
        throws DatabaseException {

        checkState(false);
        DatabaseUtil.checkForNullDbt(key, "key", true);
        DatabaseUtil.checkForNullDbt(data, "data", true);
        DatabaseUtil.checkForPartialKey(key);
        checkUpdatesAllowed("putNoDupData");
        trace(Level.FINEST, "Cursor.putNoDupData: ", key, data, null);

        return putInternal(key, data, PutMode.NODUP);
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public OperationStatus putCurrent(DatabaseEntry data)
        throws DatabaseException {

        checkState(true);
        DatabaseUtil.checkForNullDbt(data, "data", true);
        checkUpdatesAllowed("putCurrent");
        trace(Level.FINEST, "Cursor.putCurrent: ", null, data, null);

        return putInternal(null, data, PutMode.CURRENT);
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public OperationStatus getCurrent(DatabaseEntry key,
                                      DatabaseEntry data,
                                      LockMode lockMode)
        throws DatabaseException {

        checkState(true);
        checkArgsNoValRequired(key, data);
        trace(Level.FINEST, "Cursor.getCurrent: ", lockMode);

        return getCurrentInternal(key, data, lockMode);
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public OperationStatus getFirst(DatabaseEntry key,
                                    DatabaseEntry data,
                                    LockMode lockMode)
        throws DatabaseException {

        checkState(false);
        checkArgsNoValRequired(key, data);
        trace(Level.FINEST, "Cursor.getFirst: ",lockMode);

        return position(key, data, lockMode, true);
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public OperationStatus getLast(DatabaseEntry key,
                                   DatabaseEntry data,
                                   LockMode lockMode)
        throws DatabaseException {

        checkState(false);
        checkArgsNoValRequired(key, data);
        trace(Level.FINEST, "Cursor.getLast: ", lockMode);

        return position(key, data, lockMode, false);
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public OperationStatus getNext(DatabaseEntry key,
                                   DatabaseEntry data,
                                   LockMode lockMode) 
        throws DatabaseException {

        checkState(false);
        checkArgsNoValRequired(key, data);
        trace(Level.FINEST, "Cursor.getNext: ", lockMode);

        if (cursorImpl.isNotInitialized()) {
            return position(key, data, lockMode, true);
        } else {
            return retrieveNext(key, data, lockMode, GetMode.NEXT);
        }
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public OperationStatus getNextDup(DatabaseEntry key,
                                      DatabaseEntry data,
                                      LockMode lockMode)
        throws DatabaseException {

        checkState(true);
        checkArgsNoValRequired(key, data);
        trace(Level.FINEST, "Cursor.getNextDup: ", lockMode);

        return retrieveNext(key, data, lockMode, GetMode.NEXT_DUP);
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public OperationStatus getNextNoDup(DatabaseEntry key,
                                        DatabaseEntry data,
                                        LockMode lockMode)
        throws DatabaseException {

        checkState(false);
        checkArgsNoValRequired(key, data);
        trace(Level.FINEST, "Cursor.getNextNoDup: ", lockMode);

        if (cursorImpl.isNotInitialized()) {
            return position(key, data, lockMode, true);
        } else {
            return retrieveNext(key, data, lockMode, GetMode.NEXT_NODUP);
        }
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public OperationStatus getPrev(DatabaseEntry key,
                                   DatabaseEntry data,
                                   LockMode lockMode) 
        throws DatabaseException {

        checkState(false);
        checkArgsNoValRequired(key, data);
        trace(Level.FINEST, "Cursor.getPrev: ", lockMode);

        if (cursorImpl.isNotInitialized()) {
            return position(key, data, lockMode, false);
        } else {
            return retrieveNext(key, data, lockMode, GetMode.PREV);
        }
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public OperationStatus getPrevDup(DatabaseEntry key,
                                      DatabaseEntry data,
                                      LockMode lockMode)
        throws DatabaseException {

        checkState(true);
        checkArgsNoValRequired(key, data);
        trace(Level.FINEST, "Cursor.getPrevDup: ", lockMode);

        return retrieveNext(key, data, lockMode, GetMode.PREV_DUP);
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public OperationStatus getPrevNoDup(DatabaseEntry key,
                                        DatabaseEntry data,
                                        LockMode lockMode)
        throws DatabaseException {

        checkState(false);
        checkArgsNoValRequired(key, data);
        trace(Level.FINEST, "Cursor.getPrevNoDup: ", lockMode);

        if (cursorImpl.isNotInitialized()) {
            return position(key, data, lockMode, false);
        } else {
            return retrieveNext(key, data, lockMode, GetMode.PREV_NODUP);
        }
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public OperationStatus getSearchKey(DatabaseEntry key,
                                        DatabaseEntry data,
                                        LockMode lockMode)
        throws DatabaseException {

        checkState(false);
        DatabaseUtil.checkForNullDbt(key, "key", true);
        DatabaseUtil.checkForNullDbt(data, "data", false);
        trace(Level.FINEST, "Cursor.getSearchKey: ", key, null, lockMode);

        return search(key, data, lockMode, SearchMode.SET);
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public OperationStatus getSearchKeyRange(DatabaseEntry key,
                                             DatabaseEntry data,
                                             LockMode lockMode)
        throws DatabaseException {

        checkState(false);
        DatabaseUtil.checkForNullDbt(key, "key", true);
        DatabaseUtil.checkForNullDbt(data, "data", false);
        trace(Level.FINEST, "Cursor.getSearchKeyRange: ", key, null, lockMode);

        return search(key, data, lockMode, SearchMode.SET_RANGE);
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public OperationStatus getSearchBoth(DatabaseEntry key,
                                         DatabaseEntry data,
                                         LockMode lockMode)
        throws DatabaseException {

        checkState(false);
        checkArgsValRequired(key, data);
        trace(Level.FINEST, "Cursor.getSearchBoth: ", key, data, lockMode);

        return search(key, data, lockMode, SearchMode.BOTH);
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public OperationStatus getSearchBothRange(DatabaseEntry key,
                                              DatabaseEntry data,
                                              LockMode lockMode)
        throws DatabaseException {

        checkState(false);
        checkArgsValRequired(key, data);
        trace(Level.FINEST, "Cursor.getSearchBothRange: ", key, data,
              lockMode);

        return search(key, data, lockMode, SearchMode.BOTH_RANGE);
    }

    /**
     * Counts duplicates without parameter checking.
     */
    int countInternal()
        throws DatabaseException {
        
        CursorImpl original = null;
        CursorImpl dup = null;

        try {
            // Latch and clone
            original = cursorImpl;
            original.latchBINs();
            dup = original.cloneCursor();
            dup.addCursor();
            original.releaseBINs();

            int count = dup.count();
            return count;
        } finally {
            if (original != null) {
                original.releaseBINs();
            }
	    dup.close();
        }
    }

    /**
     * Internal version of delete() that does no parameter checking.  Calls
     * deleteNoNotify() and notifies triggers (performs secondary updates).
     */
    OperationStatus deleteInternal()
        throws DatabaseException {

        // Get existing data if updating secondaries.
        DatabaseEntry oldKey = null;
        DatabaseEntry oldData = null;
        boolean doNotifyTriggers = dbHandle != null && dbHandle.hasTriggers();
        if (doNotifyTriggers) {
            oldKey = new DatabaseEntry();
            oldData = new DatabaseEntry();
            OperationStatus status = getCurrentInternal(oldKey, oldData,
                                                        LockMode.RMW);
            if (status != OperationStatus.SUCCESS) {
                return OperationStatus.KEYEMPTY;
            }
        }

        /*
         * Notify triggers before the actual deletion so that a primary record
         * never exists while secondary keys refer to it.  This is relied on by
         * secondary dirty-reads.
         */
        if (doNotifyTriggers) {
            dbHandle.notifyTriggers(cursorImpl.getLocker(),
                                    oldKey, oldData, null);
        }

        /* The actual deletion. */
        OperationStatus status = deleteNoNotify();
        return status;
    }

    /**
     * Clone the cursor, delete at current position, and if successful, swap
     * cursors.  Does not notify triggers (does not perform secondary updates).
     */
    OperationStatus deleteNoNotify()
        throws DatabaseException {

        CursorImpl original = null;
        CursorImpl dup = null;
	OperationStatus status = OperationStatus.KEYEMPTY;
        try {
            // Clone, add dup to cursor
            original = cursorImpl;
            original.latchBINs();
            dup = original.cloneCursor();
            dup.addCursor();
            original.releaseBINs();

            // Latch the bins and do the delete with the dup
            dup.latchBINs();
            status = dup.delete();

            return status;
        } finally {
            if (original != null) {
                original.releaseBINs();
            }
            if (dup != null) {
                dup.releaseBINs();
            }

            // Swap if it was a success
            if (status == OperationStatus.SUCCESS) {
                original.close();
                cursorImpl = dup;
            } else {
                dup.close();
            }
        }
    }

    /**
     * Internal version of put() that does no parameter checking.  Calls
     * putNoNotify() and notifies triggers (performs secondary updates).
     */
    OperationStatus putInternal(DatabaseEntry key, DatabaseEntry data,
                                PutMode putMode)
        throws DatabaseException {

        // Need to get existing data if updating secondaries.
        DatabaseEntry oldData = null;
        boolean doNotifyTriggers = dbHandle != null && dbHandle.hasTriggers();
        if (doNotifyTriggers && (putMode == PutMode.CURRENT ||
                                 putMode == PutMode.OVERWRITE)) {
            oldData = new DatabaseEntry();
            if (key == null && putMode == PutMode.CURRENT) {
                /* Key is returned by CursorImpl.putCurrent as foundKey. */
                key = new DatabaseEntry();
            }
        }

        // Perform put
        OperationStatus commitStatus =
	    putNoNotify(key, data, putMode, oldData);

        // Notify triggers (update secondaries)
        if (doNotifyTriggers && commitStatus == OperationStatus.SUCCESS) {
            if (oldData != null && oldData.getData() == null) {
                oldData = null;
            }
            dbHandle.notifyTriggers(cursorImpl.getLocker(), key,
                                    oldData, data);
        }
        return commitStatus;
    }

    /**
     * Clone the cursor, put key/data according to PutMode, and if successful,
     * swap cursors.  Does not notify triggers (does not perform secondary
     * updates).
     */
    OperationStatus putNoNotify(DatabaseEntry key,
				DatabaseEntry data,
                                PutMode putMode,
                                DatabaseEntry returnOldData)
        throws DatabaseException {

        if (data == null) {
            throw new NullPointerException
                ("put passed a null DatabaseEntry arg");
        }

        if (putMode != PutMode.CURRENT && key == null) {
            throw new IllegalArgumentException
                ("put passed a null DatabaseEntry arg");
        }

        CursorImpl original = null;
        OperationStatus status = OperationStatus.NOTFOUND;
        CursorImpl dup = null;
        try {
            /* Latch and clone. */
            original = cursorImpl;
            original.latchBINs();
            dup = original.cloneCursor();
            if (putMode == PutMode.CURRENT) {
                dup.addCursor(); // Don't add for inserts.
            }
            original.releaseBINs();

            /* Perform operation. */
            if (putMode == PutMode.CURRENT) {
                status = dup.putCurrent(data, key, returnOldData);
            } else if (putMode == PutMode.OVERWRITE) {
                status = dup.put(key, data, returnOldData);
            } else if (putMode == PutMode.NOOVERWRITE) {
                status = dup.putNoOverwrite(key, data);
            } else if (putMode == PutMode.NODUP) {
                status = dup.putNoDupData(key, data);
            } else {
                throw new InternalException("unknown PutMode");
            }
                    
            return status;
        } finally {
            if (original != null) {
                original.releaseBINs();
            }

            if (status == OperationStatus.SUCCESS) {
                original.close();
                cursorImpl = dup;
            } else {
                if (dup != null) {
                    dup.close();
                }
            }
        }
    }

    /**
     * Position the cursor at the first or last record of the database.
     */
    OperationStatus position(DatabaseEntry key,
			     DatabaseEntry data,
                             LockMode lockMode,
			     boolean first)
        throws DatabaseException {

        if (key == null ||
            data == null) {
            throw new NullPointerException
                ("getFirst()/getLast() called with a null DatabaseEntry arg");
        }

        OperationStatus status = OperationStatus.NOTFOUND;
        CursorImpl dup = null;
        try {

            /*
             * Pass false: no need to call addCursor here because
             * positionFirstOrLast will be adding it after it finds the bin.
             */
            dup = beginRead(false);

            /* Search for first or last. */
            if (!dup.positionFirstOrLast(first, null)) {
                /* Tree is empty. */
                status = OperationStatus.NOTFOUND;
                assert Latch.countLatchesHeld() == 0:
                    Latch.latchesHeldToString();

            } else {
                /* Found something in this tree. */
                assert Latch.countLatchesHeld() == 1:
                    Latch.latchesHeldToString();
                status = dup.getCurrentAlreadyLatched(key, data,
                                                      lockMode, first);

                if (status == OperationStatus.SUCCESS) {
		    if (dup.getDupBIN() != null) {
			dup.incrementLNCount();
		    }
                } else {
                    /* The record we're pointing at may be deleted. */
                    status =
			dup.getNext(key, data, lockMode, first, false);
		}
            }
                    
            return status;
        } finally {

            /*
             * positionFirstOrLast returns with the target BIN latched, so it
             * is the responsibility of this method to make sure the latches
             * are released.
             */
            cursorImpl.releaseBINs();
            endRead(dup, status == OperationStatus.SUCCESS);
        }
    }

    /**
     * Position the cursor at target record.
     * Return with BIN of dup cursor latched.
     */
    OperationStatus search(DatabaseEntry key, DatabaseEntry data,
                           LockMode lockMode, SearchMode searchMode)
        throws DatabaseException {

        if (key == null ||
            data == null) {
            throw new NullPointerException
                ("getSearch passed a null DatabaseEntry arg");
        }

        OperationStatus status = OperationStatus.NOTFOUND;
        CursorImpl dup = null;

        try {

            /*
             * Pass false: no need to call addCursor here because
             * searchAndPosition will be adding it after it finds the bin.
             */
            dup = beginRead(false);

            /* search */
            int searchResult =
                dup.searchAndPosition(key, data, searchMode, lockMode);
            if ((searchResult & CursorImpl.FOUND) != 0) {

                /*
                 * The search found a possibly valid record.
                 * CursorImpl.searchAndPosition's job is to settle the cursor
                 * at a particular location on a BIN. In some cases, the
                 * current position may not actually hold a valid record, so
                 * it's this layer's responsiblity to judge if it might need to
                 * bump the cursor along and search more. For example, we might
                 * have to do so if the position holds a deleted record.
		 *
                 * Advance the cursor if:
                 * 
                 * 1. This is a range type search and there was no match on the
                 * search criteria (the key or key and data depending on the
                 * type of search). Then we search forward until there's a
                 * match.
                 * 
                 * 2. If this is not a range type search, check the record at
                 * the current position. If this is not a duplicate set,
                 * CursorImpl.searchAndPosition gave us an exact answer.
                 * However since it doesn't peer into the duplicate set, we may
                 * need to probe further in if there are deleted records in the
                 * duplicate set. i.e, we have to be able to find k1/d2 even if
                 * there's k1/d1(deleted), k1/d2, k1/d3, etc in a duplicate
                 * set.
		 *
		 * Note that searchResult has three bits possibly set: * FOUND
                 * has already been checked above.
                 * 
		 * EXACT means an exact match on the key portion was made.
                 * 
		 * EXACT_DATA means that if searchMode was BOTH or BOTH_RANGE
		 * then an exact match was made on the data (in addition to the
		 * key).
                 */
		boolean exactMatch =
		    ((searchResult & CursorImpl.EXACT) != 0);
		boolean exactDataMatch =
		    ((searchResult & CursorImpl.EXACT_DATA) != 0);

		/*
		 * rangeMatch means that a range match of some sort (either
		 * SET_RANGE or BOTH_RANGE) was specified and there wasn't a
		 * complete match.  If SET_RANGE was spec'd and EXACT was not
		 * returned as set, then the key didn't match exactly.  If
		 * BOTH_RANGE was spec'd and EXACT_DATA was not returned as
		 * set, then the data didn't match exactly.
		 */
		boolean rangeMatch = false;
		if (searchMode == SearchMode.SET_RANGE &&
		    !exactMatch) {
		    rangeMatch = true;
		}

		if (searchMode == SearchMode.BOTH_RANGE &&
		    !exactDataMatch) {
		    rangeMatch = true;
		}

                /* 
                 * Pass null for key to getCurrentAlreadyLatched if * useKey is
		 * SET since key is not supposed to be set * in that case.
                 */
                DatabaseEntry useKey =
                    (searchMode == SearchMode.SET) ?
                    null : key;

		/*
		 * rangeMatch => an exact match was not found so we need to
		 * advance the cursor to a real item using getNextXXX.  If
		 * rangeMatch is true, then cursor is currently on some entry,
		 * but that entry might be deleted.  So side effect status
		 * here, try to get the entry that the cursor refers to.  If
		 * that entry is deleted, then use getNextXXX to advance to a
		 * non-deleted entry.
		 */
                if (rangeMatch ||
                    (status = dup.getCurrentAlreadyLatched
                     (useKey, data, lockMode, true)) ==
                    OperationStatus.KEYEMPTY) {
                    if (searchMode == SearchMode.SET_RANGE) {
                        status = dup.getNext(key, data, lockMode,
                                             true, rangeMatch);
		    } else if (searchMode == SearchMode.SET) {
                        status = dup.getNextDuplicate(key, data, lockMode,
                                                      true, rangeMatch);
                    } else if (searchMode == SearchMode.BOTH_RANGE) {
			if (exactMatch) {
			    boolean alreadyLatched =
				(status != OperationStatus.KEYEMPTY);
			    status =
				dup.getNextDuplicate(key, data, lockMode,
						     true, alreadyLatched);

			    /*
			     * BOTH_RANGE was spec'd and the key matched
			     * exactly, but the data didn't match.  We just
			     * tried getNextDuplicate but that returned
			     * NOTFOUND.  Either there is nothing left to
			     * return, or this is the case where there is no
			     * duplicate tree and getNextDuplicate returned
			     * NOTFOUND to indicate this.  If no dup tree
			     * exists, there may still be a single entry that
			     * matches key and data.  Try to get that using
			     * getCurrent and return that status.  See SR
			     * #9428.
			     */
			    if (status == OperationStatus.NOTFOUND &&
				!exactDataMatch) {
				status = dup.getCurrentAlreadyLatched
				    (useKey, data, lockMode, true);
			    }
			} else {
			    status = dup.getNext(key, data, lockMode,
						 true, rangeMatch);
			}
                    }
                }
            }
            
            return status;
        } finally {

            /*
             * searchAndPosition returns with the target BIN latched, so it is
             * the responsibility of this method to make sure the latches are
             * released.
             */
            cursorImpl.releaseBINs();
            if (status != OperationStatus.SUCCESS && dup != cursorImpl) {
                dup.releaseBINs();
            }
            endRead(dup, status == OperationStatus.SUCCESS);
        }
    }

    /**
     * Retrieve the next record. 
     */
    OperationStatus retrieveNext(DatabaseEntry key, DatabaseEntry data,
                                 LockMode lockMode, GetMode getMode)
        throws DatabaseException {

        if (key == null ||
            data == null) {
            throw new NullPointerException
                ("getNext passed a null DatabaseEntry arg");
        }

        while (true) {
            assert Latch.countLatchesHeld() == 0;
            /* Pass true to call addCursor after duping. */
            CursorImpl dup = beginRead(true);
	    OperationStatus ret;

	    try {
		if (getMode == GetMode.NEXT) {
		    ret = dup.getNext(key, data,
				      lockMode, true, false);
		} else if (getMode == GetMode.PREV) {
		    ret = dup.getNext(key, data,
				      lockMode, false, false);
		} else if (getMode == GetMode.NEXT_DUP) {
		    ret = dup.getNextDuplicate(key, data,
					       lockMode, true, false);
		} else if (getMode == GetMode.PREV_DUP) {
		    ret = dup.getNextDuplicate(key, data,
					       lockMode, false, false);
		} else if (getMode == GetMode.NEXT_NODUP) {
		    ret = dup.getNextNoDup(key, data, lockMode, true);
		} else if (getMode == GetMode.PREV_NODUP) {
		    ret = dup.getNextNoDup(key, data, lockMode, false);
		} else {
		    throw new InternalException("unknown GetMode");
		}
	    } catch (DatabaseException DBE) {
                // bozo
                DBE.printStackTrace();
                endRead(dup, false);
		throw DBE;
	    }

            if (checkForInsertion(getMode, cursorImpl, dup)) {
                endRead(dup, false);
                continue;
            }

            endRead(dup, ret == OperationStatus.SUCCESS);

            assert Latch.countLatchesHeld() == 0;
            return ret;
        }
    }

    /**
     * Returns the current key and data.
     */
    OperationStatus getCurrentInternal(DatabaseEntry key,
                                       DatabaseEntry data,
                                       LockMode lockMode)
        throws DatabaseException {

        return cursorImpl.getCurrent(key, data, lockMode);
    }

    /*
     * Something may have been added to the original cursor (cursorImpl) while
     * we were getting the next BIN.  cursorImpl would have been adjusted
     * properly but we would have skipped a BIN in the process.
     *
     * Note that when we call LN.isDeleted(), we do not need to lock the LN.
     * If we see a non-committed deleted entry, we'll just iterate around in
     * the caller.  So a false positive is ok.
     *
     * @return true if an unaccounted for insertion happened.
     */
    private boolean checkForInsertion(GetMode getMode,
                                      CursorImpl origCursor,
                                      CursorImpl dupCursor)
        throws DatabaseException {

        BIN origBIN = origCursor.getBIN();
        BIN dupBIN = dupCursor.getBIN();
        DBIN origDBIN = origCursor.getDupBIN();

        boolean forward = true;
        if (getMode == GetMode.PREV ||
            getMode == GetMode.PREV_DUP ||
            getMode == GetMode.PREV_NODUP) {
            forward = false;
        }
        boolean ret = false;
        if (origBIN != dupBIN) {
            /* We jumped to the next BIN during getNext(). */
            origCursor.latchBINs();
            if (origDBIN == null) {
                if (forward) {
                    if (origBIN.getNEntries() - 1 > origCursor.getIndex()) {

                        /* 
                         * We were adjusted to something other than the last
                         * entry so some insertion happened.
                         */
                        DatabaseImpl database = origBIN.getDatabase();
                        for (int i = origCursor.getIndex() + 1;
                             i < origBIN.getNEntries();
                             i++) {
                            ChildReference entry = origBIN.getEntry(i);
                            if (!entry.isKnownDeleted()) {
                                Node n = entry.fetchTarget(database, origBIN);
                                if (!n.containsDuplicates()) {
                                    LN ln = (LN) n;
                                    /* See comment above about locking. */
                                    if (!ln.isDeleted()) {
                                        ret = true;
                                        break;
                                    }
                                }
                            } else {
                                /* Need to check the DupCountLN. */
                            }
                        }
                    }
                } else {
                    if (origCursor.getIndex() > 0) {

                        /*
                         * We were adjusted to something other than the first
                         * entry so some insertion happened.
                         */
                        DatabaseImpl database = origBIN.getDatabase();
                        for (int i = 0; i < origCursor.getIndex(); i++) {
                            ChildReference entry = origBIN.getEntry(i);
                            if (!entry.isKnownDeleted()) {
                                Node n = entry.fetchTarget(database, origBIN);
                                if (!n.containsDuplicates()) {
                                    LN ln = (LN) n;
                                    /* See comment above about locking. */
                                    if (!ln.isDeleted()) {
                                        ret = true;
                                        break;
                                    }
                                } else {
                                    /* Need to check the DupCountLN. */
                                }
                            }
                        }
                    }
                }
            }
            origCursor.releaseBINs();
            return ret;
        }

        if (origDBIN != dupCursor.getDupBIN() &&
            origCursor.getIndex() == dupCursor.getIndex() &&
            getMode != GetMode.NEXT_NODUP &&
            getMode != GetMode.PREV_NODUP) {
            /* Same as above, only for the dupBIN. */
            origCursor.latchBINs();
            if (forward) {
                if (origDBIN.getNEntries() - 1 >
                    origCursor.getDupIndex()) {

                    /* 
                     * We were adjusted to something other than the last entry
                     * so some insertion happened.
                     */
                    DatabaseImpl database = origDBIN.getDatabase();
                    for (int i = origCursor.getDupIndex() + 1;
                         i < origDBIN.getNEntries();
                         i++) {
                        ChildReference entry = origDBIN.getEntry(i);
                        if (!entry.isKnownDeleted()) {
                            Node n = entry.fetchTarget(database, origDBIN);
                            LN ln = (LN) n;
                            /* See comment above about locking. */
                            if (!ln.isDeleted()) {
                                ret = true;
                                break;
                            }
                        }
                    }
                }
            } else {
                if (origCursor.getDupIndex() > 0) {

                    /*
                     * We were adjusted to something other than the first entry
                     * so some insertion happened.
                     */
                    DatabaseImpl database = origDBIN.getDatabase();
                    for (int i = 0; i < origCursor.getDupIndex(); i++) {
                        ChildReference entry = origDBIN.getEntry(i);
                        if (!entry.isKnownDeleted()) {
                            Node n = entry.fetchTarget(database, origDBIN);
                            LN ln = (LN) n;
                            /* See comment above about locking. */
                            if (!ln.isDeleted()) {
                                ret = true;
                                break;
                            }
                        }
                    }
                }
            }
            origCursor.releaseBINs();
            return ret;
        }
        return false;
    }

    /**
     * If the cursor is initialized, dup it and return the dup; otherwise,
     * return the original.  This avoids the overhead of duping when the
     * original is uninitialized.  The cursor returned must be passed to
     * endRead() to close the correct cursor.
     */
    private CursorImpl beginRead(boolean addCursor)
        throws DatabaseException {

        CursorImpl dup;
        if (cursorImpl.isNotInitialized()) {
            dup = cursorImpl;
        } else {
            cursorImpl.latchBINs();
            dup = cursorImpl.cloneCursor();
            if (addCursor) {
                dup.addCursor();
            }
            cursorImpl.releaseBINs();
        }
        return dup;
    }

    /**
     * If the operation is successful, swaps cursors and close the original
     * cursor; otherwise, closes the duped cursor.  In the case where the
     * original cursor was not duped by beginRead because it was uninitialized,
     * just resets the original cursor if the operation did not succeed.
     */
    private void endRead(CursorImpl dup, boolean success)
        throws DatabaseException {

        if (dup == cursorImpl) {
            if (!success) {
                cursorImpl.reset();
            }
        } else {
            if (success) {
                cursorImpl.close();
                cursorImpl = dup;
            } else {
                dup.close();
            }
        }
    }

    private void checkUpdatesAllowed(String operation)
        throws DatabaseException {

        if (updateOperationsProhibited) {
            throw new DatabaseException
                ("A transaction was not supplied when opening this cursor: " +
                 operation);
        }
    }

    /**
     * Note that this flavor of checkArgs doesn't require that the dbt data is
     * set.
     */
    private void checkArgsNoValRequired(DatabaseEntry key,
                                        DatabaseEntry data) {
        DatabaseUtil.checkForNullDbt(key, "key", false);
        DatabaseUtil.checkForNullDbt(data, "data", false);
    }

    /**
     * Note that this flavor of checkArgs requires that the dbt data is set.
     */
    private void checkArgsValRequired(DatabaseEntry key,
                                      DatabaseEntry data) {
        DatabaseUtil.checkForNullDbt(key, "key", true);
        DatabaseUtil.checkForNullDbt(data, "data", true);
    }

    /**
     * Check the environment and cursor state.
     */
    void checkState(boolean mustBeInitialized)
        throws DatabaseException {

        checkEnv();
        cursorImpl.checkCursorState(mustBeInitialized);
    }

    /**
     * @throws RunRecoveryException if the underlying environment is invalid.
     */
    void checkEnv() 
        throws RunRecoveryException {

        cursorImpl.checkEnv();
    }

    /**
     * Send trace messages to the java.util.logger. Don't rely on the logger
     * alone to conditionalize whether we send this message, we don't even want
     * to construct the message if the level is not enabled.
     */
    void trace(Level level,
               String methodName,
               DatabaseEntry key,
               DatabaseEntry data,
               LockMode lockMode) {
        Logger logger = dbImpl.getDbEnvironment().getLogger();
        if (logger.isLoggable(level)) {
            StringBuffer sb = new StringBuffer();
            sb.append(methodName);
            traceCursorImpl(sb);
            if (key != null) {
                sb.append(" key=").append(key.dumpData());
            }
            if (data != null) {
                sb.append(" data=").append(data.dumpData());
            }
            if (lockMode != null) {
                sb.append(" lockMode=").append(lockMode);
            }
            logger.log(level, sb.toString());
        }
    }
    /**
     * Send trace messages to the java.util.logger. Don't rely on the logger
     * alone to conditionalize whether we send this message, we don't even want
     * to construct the message if the level is not enabled.
     */
    void trace(Level level, String methodName, LockMode lockMode) {
        Logger logger = dbImpl.getDbEnvironment().getLogger();
        if (logger.isLoggable(level)) {
            StringBuffer sb = new StringBuffer();
            sb.append(methodName);
            traceCursorImpl(sb);
            if (lockMode != null) {
                sb.append(" lockMode=").append(lockMode);
            }
            logger.log(level, sb.toString());
        }
    }

    private void traceCursorImpl(StringBuffer sb) {
        sb.append(" locker=").append(cursorImpl.getLocker().getId());
        if (cursorImpl.getBIN() != null) {
            sb.append(" bin=").append(cursorImpl.getBIN().getNodeId());
        }
        sb.append(" idx=").append(cursorImpl.getIndex());
        
        if (cursorImpl.getDupBIN() != null) {
            sb.append(" Dbin=").append(cursorImpl.getDupBIN().getNodeId());
        }
        sb.append(" dupIdx=").append(cursorImpl.getDupIndex());
    }
}
