/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: Txn.java,v 1.1 2004/11/22 18:27:57 kate Exp $
*/

package com.sleepycat.je.txn;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.LockStats;
import com.sleepycat.je.RunRecoveryException;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.dbi.CursorImpl;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.latch.Latch;
import com.sleepycat.je.log.LogManager;
import com.sleepycat.je.log.LogReadable;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.log.LogWritable;
import com.sleepycat.je.log.entry.LNLogEntry;
import com.sleepycat.je.recovery.RecoveryManager;
import com.sleepycat.je.tree.LN;
import com.sleepycat.je.tree.TreeLocation;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.Tracer;

/**
 * A Txn is one that's created by a call to Environment.txnBegin.
 * This class must support multithreaded use.
 */
public class Txn extends Locker implements LogWritable, LogReadable {
    private static final String DEBUG_NAME =
        Txn.class.getName();

    private byte txnState;
    private Set cursors;     // cursors opened under this txn
    private static final byte USABLE = 0;
    private static final byte CLOSED = 1;

    /*
     * A Txn can be used by multiple threads. 
     * lockLatch protects read and write locks. One latch used in case we 
     * promote
     */
    private Latch lockLatch;  
    private Set readLocks;
    private Map writeInfo;    // Map of WriteLockInfo, per write locked node

    /* 
     * We have to keep a set of deleted databasesImpls so after commit, we can
     * mark a flag in the object that will lead the evictor to asynchronously
     * clean up the in memory list. We do this asynchronously so as not to 
     * burden commit. Synchronize access to this set on this object.
     */
    private Set deletedDatabases; 

    /* Use this boolean to remember if we once owned a write lock for a db
     * handle, but transferred it before committing. If we did, we'll have
     * to write a commit record for this txn, and bypass the optimization that
     * skips commit records for txns with no write locks
     */
    private boolean onceOwnedDbHandleWriteLock;

    /*
     * We need a map of the latest databaseImpl objects to drive the undo
     * during an abort, because it's too hard to look up the database
     * object in the mapping tree. (The normal code paths want to take
     * locks, add cursors, etc.
     */
    private Map undoDatabases;

    /* Last lsn logged for this transaction. */
    private DbLsn lastLoggedLsn = null;

    /* First lsn logged for this transaction -- used for keeping track of
     * the first active lsn point, for checkpointing. This field is not
     * persistent.
     */
    private DbLsn firstLoggedLsn = null;

    /* Whether to sync on commit by default. */
    private boolean defaultSync;

    /**
     * Create a transaction from Environment.txnBegin.
     */
    public Txn(EnvironmentImpl envImpl, TransactionConfig config)
        throws DatabaseException {

        /*
         * Initialize using the config but don't hold a reference to it, since
         * it has not been cloned.
         */
        super(envImpl, config.getDirtyRead(), config.getNoWait());
        defaultSync = config.getSync() || !config.getNoSync();

        cursors = new HashSet();

        lockLatch = new Latch(envImpl);
        readLocks = new HashSet();
        writeInfo = new HashMap();

        /* Note: deleteDatabases is initialized lazily. */
        undoDatabases = new HashMap();

        lastLoggedLsn = DbLsn.NULL_LSN;
        firstLoggedLsn = null;

        txnState = USABLE;
        this.envImpl.getTxnManager().registerTxn(this);
        onceOwnedDbHandleWriteLock = false;
    }

    /**
     * Constructor for reading from log.
     */
    public Txn() {
        lastLoggedLsn = new DbLsn();
    }

    /**
     * UserTxns get a new unique id for each instance.
     */
    protected long generateId(TxnManager txnManager) {
        return txnManager.incTxnId();
    }

    /**
     * Access to last lsn.
     */
    DbLsn getLastLsn() {
        return lastLoggedLsn;
    }

    /**
     * Get a write lock on this LN and save an abort lsn.  Caller will
     * set the abortLsn later, after the write lock has been obtained.
     */
    public LockResult writeLock(LN ln, DatabaseImpl database)
        throws DatabaseException {

        checkState(true);

        /* Ask for the lock. */
        LockGrantType grant =
            lockManager.lock(ln.getNodeId(), this,
                             LockType.WRITE,
                             lockTimeOutMillis,
                             defaultNoWait);
    
        /*
         * The lock was granted if no exception was thrown.  Register the
         * lock, the node's database and the node's original lsn.
         */
        lockLatch.acquire();

        /* Save the oldLsn if this is the first write lock */
	WriteLockInfo info =
            (WriteLockInfo) writeInfo.get(new Long(ln.getNodeId()));

        /* Save the latest version of this database for undoing. */
        undoDatabases.put(database.getId(), database);

        lockLatch.release();
	return new LockResult(grant, info);
    }

    /**
     * Call commit() with the default sync configuration property.
     */
    public DbLsn commit()
        throws DatabaseException {

        return commit(defaultSync);
    }

    /**
     * Commit this transaction
     * 1. Releases read locks
     * 2. Writes a txn commit record into the log
     * 3. Flushes the log to disk.
     * 4. Add deleted LN info to IN compressor queue
     * 5. Release all write locks 
     * 
     * If any step of this fails, we must convert this transaction to an abort
     */
    public DbLsn commit(boolean doSync)
        throws DatabaseException {

        checkState(true);
        synchronized (this) {
            try {
                if (checkCursorsForClose()) {
                    throw new DatabaseException
                        ("Transaction " + id +
                         " commit failed because there were open cursors");
                }

                /* Transfer handle locks to their owning handles. */
                if (handleLockToHandleMap != null) {
                    Iterator handleLockIter =
                        handleLockToHandleMap.entrySet().iterator(); 
                    while (handleLockIter.hasNext()){
                        Map.Entry entry = (Map.Entry) handleLockIter.next();
                        transferHandleLockToHandleSet((Long) entry.getKey(),
                                                      (Set) entry.getValue());
                    }
                }

                LogManager logManager = envImpl.getLogManager();

                int numWriteLocks = writeInfo.size();
                int numReadLocks = readLocks.size();
                        
                lockLatch.acquire();
                DbLsn commitLsn = null;

                try {
                    /* 
                     * Release all read locks, clear lock collection. Optimize
                     * for the case where there are no read locks.
                     */
                    if (numReadLocks > 0) {
                        Iterator iter = readLocks.iterator();
                        while (iter.hasNext()) {
                            Lock rLock = (Lock) iter.next();
                            lockManager.release(rLock, this);
                        }
                        readLocks.clear();
                    }

                    /* Log the commit if there were any write locks. */
                    if ((numWriteLocks > 0) || onceOwnedDbHandleWriteLock) {
                        TxnCommit commitRecord =
			    new TxnCommit(id, lastLoggedLsn);
                        if (doSync) {
                            commitLsn = logManager.logForceFlush(commitRecord);
                        } else {
                            commitLsn = logManager.log(commitRecord);
                        }
                
                        /* Release all write locks, clear lock collection. */
                        Iterator iter = writeInfo.values().iterator();
                        while (iter.hasNext()) {
                            WriteLockInfo info = (WriteLockInfo) iter.next();
                            lockManager.release(info.lock, this);
                        }
                        writeInfo.clear();
                    }
                } finally {
                    lockLatch.release();
                }

                if (numWriteLocks > 0) {
                    // Unload delete info.
                    synchronized (this) {
                        if ((deleteInfo != null) && deleteInfo.size() > 0) {
                            envImpl.addToCompressorQueue(deleteInfo.values());
                            deleteInfo.clear();
                        }

                        if (deletedDatabases != null) {
                            Iterator iter = deletedDatabases.iterator();
                            while (iter.hasNext()) {
                                DatabaseImpl db = (DatabaseImpl) iter.next();
                                db.deleteAndReleaseINs();
                            }
                            deletedDatabases.clear();
                        }
                    }
                }

                traceCommit(numWriteLocks, numReadLocks);

                /* Unregister this txn. */
                close(true);
                return commitLsn;
            } catch (RunRecoveryException e) {

                /* May have received a thread interrupt. */
                throw e;
            } catch (Throwable t) {

                try {
                    if (lockLatch.isOwner()) {
                        lockLatch.release();
                    }

                    abort();
                    Tracer.trace(envImpl, "Txn", "commit",
                                 "Commit of transaction " + id + " failed", t);
                } catch (Throwable abortT2) {
                    throw new DatabaseException
                        ("Failed while attempting to commit transaction " +
                         id +
                         ". The attempt to abort and clean up also failed. " +
                         "The original exception seen from commit = " +
                         t.getMessage() +
                         " The exception from the cleanup = " +
                         abortT2.getMessage(),
                         t);
                }
                
                /* Now throw an exception that shows the commit problem. */
                throw new DatabaseException
                    ("Failed while attempting to commit transaction " + id +
                     ", aborted instead. Original exception = " +
                     t.getMessage(), t);
            }
        } 
    }

    /**
     * Abort this transaction. Steps are:
     * 1. Release LN read locks.
     * 2. Write a txn abort entry to the log. This is only for log
     *    file cleaning optimization and there's no need to guarantee a
     *    flush to disk.  
     * 3. Find the last ln log entry written for this txn, and use that
     *    to traverse the log looking for nodes to undo. For each node,
     *    use the same undo logic as recovery to rollback the transaction. Note
     *    that we walk the log in order to undo in reverse order of the
     *    actual operations. For example, suppose the txn did this:
     *       delete K1/D1 (in LN 10)
     *       create K1/D1 (in LN 20)
     *    If we process LN10 before LN 20, we'd inadvertently create a 
     *    duplicate tree of "K1", which would be fatal for the mapping tree.
     * 4. Release the write lock for this LN.
     */
    public DbLsn abort()
        throws DatabaseException {

        synchronized (this) {
            try {
                checkState(false);

                lockLatch.acquire();
                int numWriteLocks = writeInfo.size();
                int numReadLocks = readLocks.size();

                // Log the abort.
                TxnAbort abortRecord = new TxnAbort(id, lastLoggedLsn);
                DbLsn abortLsn = null;
                if (numWriteLocks > 0) {
                    abortLsn = envImpl.getLogManager().log(abortRecord);
                }

                // Undo the changes.
                undo();

                /*
                 * Release all read locks after the undo (since the undo may
                 * need to read in mapLNs.
                 */
                Iterator iter = readLocks.iterator();
                while (iter.hasNext()) {
                    Lock rLock = (Lock) iter.next();
                    lockManager.release(rLock, this);
                }
                readLocks.clear();

                // Throw away write lock collection.
                iter = writeInfo.values().iterator();
                while (iter.hasNext()) {
                    WriteLockInfo info = (WriteLockInfo) iter.next();
                    lockManager.release(info.lock, this);
                }

                writeInfo.clear();
                lockLatch.release();

                /*
                 * Let the delete related info (binreferences and dbs) get
                 * gc'ed. Don't explicitly iterate and clear -- that's far
                 * less efficient, gives GC wrong input.
                 */
                synchronized (this) {
                    deleteInfo = null;
                    deletedDatabases = null;
                }

                boolean openCursors = checkCursorsForClose();
                Tracer.trace(Level.FINE,
                             envImpl,
                             "Abort:id = " + id +
                             " numWriteLocks= " + numWriteLocks +
                             " numReadLocks= " + numReadLocks +
                             " openCursors= " + openCursors);
                if (openCursors) {
                    throw new DatabaseException
                        ("Transaction " + id +
                         " detected open cursors while aborting");
                }
                /* Unload any db handles protected by this txn. */
                if (handleToHandleLockMap != null) {
                    Iterator handleIter =
                        handleToHandleLockMap.keySet().iterator(); 
                    while (handleIter.hasNext()){
                        Database handle = (Database) handleIter.next();
                        DbInternal.dbInvalidate(handle);
                    }
                }

                return abortLsn;
            } finally {
                // unregister this txn
                close(false);
            }
        }
    }

    /**
     * Rollback the changes to this txn's write locked nodes.
     */
    private void undo() 
        throws DatabaseException {
        
        Long nodeId = null;
        DbLsn undoLsn = lastLoggedLsn;
        LogManager logManager = envImpl.getLogManager();

        try {
            Set alreadyUndone = new HashSet();
            TreeLocation location = new TreeLocation();
            while (!undoLsn.equals(DbLsn.NULL_LSN)) {

                LNLogEntry undoEntry = 
                    (LNLogEntry) logManager.getLogEntry(undoLsn);
                LN undoLN = undoEntry.getLN();
                nodeId = new Long(undoLN.getNodeId());

                /* 
                 * Only process this if this is the first time we've seen
                 * this node. All log entries for a given node have the same
                 * abortLsn, so we don't need to undo it multiple times.
                 */
                if (!alreadyUndone.contains(nodeId)) {
                    alreadyUndone.add(nodeId);
                    DatabaseId dbId = undoEntry.getDbId();
                    DatabaseImpl db = (DatabaseImpl) undoDatabases.get(dbId);
                    undoLN.postFetchInit(db);
                    DbLsn abortLsn = undoEntry.getAbortLsn();
                    boolean abortKnownDeleted =
                        undoEntry.getAbortKnownDeleted();
                    try {
                        boolean insertedOrReplaced =
                            RecoveryManager.undo(Level.FINER,
                                                 db,
                                                 location,
                                                 undoLN,
                                                 undoEntry.getKey(),
                                                 undoEntry.getDupKey(),
                                                 undoLsn,
                                                 abortLsn,
                                                 abortKnownDeleted,
                                                 null, false);
                    } finally {
                        if (location.bin != null) {
                            location.bin.releaseLatch();
                        }
                    }
            
                    /*
                     * The LN undone is counted as obsolete if it is not
                     * deleted, and the abortLsn is counted as non-obsolete
                     * (the obsolete count is decremented) if it was not
                     * previously deleted.
                     */
                    DbLsn obsoleteLsn = null;
                    DbLsn nonObsoleteLsn = null;
                    if (!undoLN.isDeleted()) {
                        obsoleteLsn = undoLsn;
                    }
                    if (abortLsn != null && !abortKnownDeleted) {
                        nonObsoleteLsn = abortLsn;
                    }
                    if (obsoleteLsn != null || nonObsoleteLsn != null) {
                        logManager.countObsoleteLNs(obsoleteLsn, true,
                                                    nonObsoleteLsn, false);
                    }
                }

                /* Move on to the previous log entry for this txn. */
                undoLsn = undoEntry.getUserTxn().getLastLsn();
            }
        } catch (DatabaseException e) {
            Tracer.trace(envImpl, "Txn", "undo", 
			 "for node=" + nodeId + " lsn=" +
			 undoLsn.getNoFormatString(), e);
            throw e;
        }
    }

    /**
     * Called by the log manager when logging a transaction aware object.
     * This method is synchronized by the caller, by being called within 
     * the log latch. Record the last lsn for this transaction, to create
     * the transaction chain, and also record the lsn in the write info
     * for abort logic.
     */
    public void addLogInfo(long nodeId, DbLsn lastLsn)
        throws DatabaseException {

        /* Save the last lsn  for maintaining the transaction lsn chain */
        lastLoggedLsn = lastLsn;

        /* Save handle to lsn for aborts. */
        lockLatch.acquire();
        WriteLockInfo info = (WriteLockInfo) writeInfo.get(new Long(nodeId));
        assert info != null;

        /* 
         * If this is the first lsn, save it for calculating the first lsn
         * of any active txn, for checkpointing.
         */

        if (firstLoggedLsn == null) {
            firstLoggedLsn = lastLsn;
        }

        lockLatch.release();
    }

    /**
     * @return first logged lsn, to aid recovery rollback.
     */
    DbLsn getFirstActiveLsn() 
        throws DatabaseException {

        DbLsn first = null;
        lockLatch.acquire();
        first = firstLoggedLsn;
        lockLatch.release();
        return first;
    }

    public void setIsDeletedAtCommit(DatabaseImpl db)
        throws DatabaseException {

        synchronized (this) {
            if (deletedDatabases == null) {
                deletedDatabases = new HashSet();
            }
            deletedDatabases.add(db);
        }
    }

    /**
     * Add lock to the appropriate queue.
     */
    void addLock(long nodeId,
		 Lock lock,
                 LockType type,
		 LockGrantType grantStatus) 
        throws DatabaseException {

        try {
            lockLatch.acquire();
            if (type == LockType.WRITE) {
                writeInfo.put(new Long(nodeId), 
                              new WriteLockInfo(lock));
                if ((grantStatus == LockGrantType.PROMOTION) ||
                    (grantStatus == LockGrantType.WAIT_PROMOTION)) {
                    readLocks.remove(lock);
                }
            } else {
                readLocks.add(lock);
            }
        } finally {
            if (lockLatch.isOwner()) {
                lockLatch.release();
            }
        }
    }

    /**
     * Remove the lock from the set owned by this transaction. If specified
     * to LockManager.release, the lock manager will call this when its
     * releasing a lock. Usually done because the transaction doesn't need
     * to really keep the lock, i.e for a deleted record.
     */
    void removeLock(long nodeId, Lock lock) 
        throws DatabaseException {

        /* 
         * We could optimize by passing the lock type so we know which
         * collection to look in. Be careful of demoted locks, which
         * have shifted collection.
         */
        lockLatch.acquire();
        if (!readLocks.remove(lock)) {
            if (writeInfo.remove(new Long(nodeId)) == null) {
                throw new DatabaseException
                    ("Couldn't find lock for Node " + nodeId +
                     " in writeInfo Map.");
            }
        }
        lockLatch.release();
    }

    /**
     * A lock is being demoted. Move it from the write collection into the
     * read collection. 
     */
    void moveWriteToReadLock(long nodeId, Lock lock)
        throws DatabaseException {

        lockLatch.acquire();
        if (writeInfo.remove(new Long(nodeId)) == null) {
            lockLatch.release();
            throw new DatabaseException
                ("Couldn't find lock for Node " + nodeId +
                 " in writeInfo Map.");
        }
        readLocks.add(lock);
        lockLatch.release();
    }

    /**
     * @return true if this transaction created this node. We know that this
     * is true if the node is write locked and has a null abort lsn.
     */
    public boolean createdNode(long nodeId) 
        throws DatabaseException {

        boolean created = false;
        lockLatch.acquire();
	try {
	    WriteLockInfo info = (WriteLockInfo)
		writeInfo.get(new Long(nodeId));
	    if (info != null) {
		created = info.createdThisTxn;
	    }
	} finally {
	    lockLatch.release();
	}
        return created;
    }

    /**
     * Let the mapLN set the undoDatabase after a rollback.
     */
    public void setUndoDatabase(DatabaseId dbId, DatabaseImpl db) {
        undoDatabases.put(dbId, db);
    }

    /**
     * @return the abortLsn for this node.
     */
    public DbLsn getAbortLsn(long nodeId) 
        throws DatabaseException {

        lockLatch.acquire();
        WriteLockInfo info = (WriteLockInfo) writeInfo.get(new Long(nodeId));
        lockLatch.release();
        if (info == null) {
            return null;
        } else {
            return info.abortLsn;
        }
    }

    /**
     * @return the abortKnownDeleted for this node.
     */
    public boolean getAbortKnownDeleted(long nodeId) 
        throws DatabaseException {

        lockLatch.acquire();
        WriteLockInfo info = (WriteLockInfo) writeInfo.get(new Long(nodeId));
        lockLatch.release();
        if (info == null) {
            return true;
        } else {
            return info.abortKnownDeleted;
        }
    }

    /**
     * Is transactional.
     */
    public boolean isTransactional() {
        return true;
    }

    /**
     * Asserts false and returns null since only ThreadLocker should be cloned.
     */
    public Locker newInstance()
        throws DatabaseException {

        assert false: "Only ThreadLocker should be cloned";
        return null;
    }

    /**
     * Returns whether this txn can share locks with the given txn.
     */
    public boolean sharesLocksWith(Locker txn) {
        return false;
    }

    /**
     * Created transactions do nothing at the end of the operation.
     */
    public void operationEnd()
        throws DatabaseException {
    }

    /**
     * Created transactions do nothing at the end of the operation.
     */
    public void operationEnd(boolean operationOK)
        throws DatabaseException {
    }

    /**
     * Created transactions don't transfer locks until commit.
     */
    public void setHandleLockOwner(boolean operationOK,
                                   Database dbHandle, 
                                   boolean dbIsClosing) 
        throws DatabaseException {

        if (dbIsClosing) {
            /* 
             * If the Database handle is closing, take it out of the both
             * the handle lock map and the handle map. We don't need to do
             * any transfers at commit time, and we don't need to do any
             * invalidations at abort time.
             */
            Long handleLockId = (Long) handleToHandleLockMap.get(dbHandle);
            if (handleLockId != null) {
                Set dbHandleSet = (Set) handleLockToHandleMap.get(handleLockId);  
                boolean removed = dbHandleSet.remove(dbHandle);
                assert removed : "Can't find " + dbHandle + " from dbHandleSet";
                if (dbHandleSet.size() == 0) {
                    Object foo = handleLockToHandleMap.remove(handleLockId);
                    assert (foo != null) : "Can't find " + handleLockId + " from handleLockIdtoHandleMap.";
                }
            }

            unregisterHandle(dbHandle);

        } else {
            /* 
             * If the db is still open, make sure the db knows this txn is
             * its handle lock protector and that this txn knows it owns this db
             * handle.
             */
            if (dbHandle != null) {
                DbInternal.dbSetHandleLocker(dbHandle, this);
            }
        }
    }


    /**
     * Cursors operating under this transaction are added to the collection.
     */
    public void registerCursor(CursorImpl cursor) 
        throws DatabaseException {

        synchronized(this) {
            cursors.add(cursor);
        }
    }
    /**
     * Remove a cursor from the collection.
     */
    public void unRegisterCursor(CursorImpl cursor) 
        throws DatabaseException {

        synchronized (this) {
            cursors.remove(cursor);
        }
    }

    /**
     * @return true if this txn is willing to give up the handle lock to
     * another txn before this txn ends.
     */
    public boolean isHandleLockTransferrable() {
        return false;
    }

    /**
     * Check if all cursors associated with the txn are closed. If not,
     * those open cursors will be forcibly closed.
     * @return true if open cursors exist
     */
    private boolean checkCursorsForClose()
        throws DatabaseException {

        Iterator iter = cursors.iterator();
        while (iter.hasNext()) {
            CursorImpl c = (CursorImpl) iter.next();
            if (!c.isClosed()) {
                return true;
            }
        }

        return false;
    }

    /**
     * Get lock stats
     */
    /**
     * stats
     */
    public LockStats collectStats(LockStats stats)
        throws DatabaseException {

        lockLatch.acquire();
        try {
            stats.setNReadLocks(stats.getNReadLocks() + readLocks.size());
            stats.setNWriteLocks(stats.getNWriteLocks() + writeInfo.size());
        } finally {
            if (lockLatch.isOwner()) {
                lockLatch.release();
            }
        }
        return stats;
    }

    /**
     * Throw an exception if the transaction is not open.
     */
    private void checkState(boolean acquireLatch)
        throws DatabaseException {

        boolean ok = false;
        synchronized (this) {
            ok = (txnState == USABLE);
        }

        if (!ok) {
	    /*
	     * It's ok for FindBugs to whine about id not being synchronized.
	     */
            throw new DatabaseException
		("Transaction " + id + " has been closed");
        }
    }

    /**
     * Assumes txnStateLatch held.
     */
    private void close(boolean isCommit)
        throws DatabaseException {

        txnState = CLOSED;
        envImpl.getTxnManager().unRegisterTxn(this, isCommit);
    }

    /**
     * Txns that commit must remember if they once owned a db handle
     * write lock, to make commit optimizations work correctly.
     */
    protected void rememberHandleWriteLock(Long handleLockId) {
        /* Do we have a write lock for this db handle? */
        if (writeInfo.get(handleLockId) != null) {
            onceOwnedDbHandleWriteLock = true;
        }
    }
    
    /*
     * Log support
     */

    /**
     * @see LogWritable#getLogSize
     */
    public int getLogSize() {
        return LogUtils.LONG_BYTES + lastLoggedLsn.getLogSize();
    }

    /**
     * @see LogWritable#writeToLog
     */
    /*
     * It's ok for FindBugs to whine about id not being synchronized.
     */
    public void writeToLog(ByteBuffer logBuffer) {
        LogUtils.writeLong(logBuffer, id);
        lastLoggedLsn.writeToLog(logBuffer);
    }

    /**
     * @see LogReadable#readFromLog
     */
    /*
     * It's ok for FindBugs to whine about id not being synchronized.
     */
    public void readFromLog(ByteBuffer logBuffer){
        id = LogUtils.readLong(logBuffer);
        lastLoggedLsn.readFromLog(logBuffer);
    }

    /**
     * @see LogReadable#dumpLog
     */
    public void dumpLog(StringBuffer sb, boolean verbose) {
        sb.append("<txn id=\"");
        sb.append(super.toString());
        sb.append("\">");
        lastLoggedLsn.dumpLog(sb, verbose);
        sb.append("</txn>");
    }

    /**
     * @see LogReadable#getTransactionId
     */
    public long getTransactionId() {
	return getId();
    }

    /**
     * @see LogReadable#logEntryIsTransactional
     */
    public boolean logEntryIsTransactional() {
	return true;
    }

    /**
     * Transfer a single handle lock to the set of corresponding handles
     * at commit time.
     */
    private void transferHandleLockToHandleSet(Long handleLockId,
                                          Set dbHandleSet) 
        throws DatabaseException {
        
        /* If needed, remember that we onced owned this lock. */
        rememberHandleWriteLock(handleLockId);

        /* Create a set of destination transactions */
        int numHandles = dbHandleSet.size();
        Database [] dbHandles = new Database[numHandles];
        dbHandles = (Database []) dbHandleSet.toArray(dbHandles);
        Locker [] destTxns = new Locker[numHandles];
        for (int i = 0; i < numHandles; i++) {
            destTxns[i] = new BasicLocker(envImpl);
        }
                
        /* Move this lock to the destination txns. */
        long nodeId = handleLockId.longValue();
        lockManager.transferMultiple(nodeId, this, destTxns);

        for (int i = 0; i < numHandles; i++) {
            /* 
             * Make this handle and its handle protector txn remember
             * each other.
             */
            destTxns[i].addToHandleMaps(handleLockId, dbHandles[i]);
            DbInternal.dbSetHandleLocker(dbHandles[i], destTxns[i]);
        }
    }

    /**
     * Conditionalize the construction of the 
     * trace string, as a performance optimization. The string construction
     * showed up on a profile.
     */
    private void traceCommit(int numWriteLocks, int numReadLocks) {
        Logger logger = envImpl.getLogger();
        if (logger.isLoggable(Level.FINE)) {
            StringBuffer sb = new StringBuffer();
            sb.append(" Commit:id = ").append(id);
            sb.append(" numWriteLocks=").append(numWriteLocks);
            sb.append(" numReadLocks = ").append(numReadLocks);
            Tracer.trace(Level.FINE, envImpl, sb.toString());
        }
    }
}
