/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: BasicLocker.java,v 1.1 2004/11/22 18:27:57 kate Exp $
*/

package com.sleepycat.je.txn;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.LockNotGrantedException;
import com.sleepycat.je.LockStats;
import com.sleepycat.je.dbi.CursorImpl;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.tree.LN;
import com.sleepycat.je.utilint.DbLsn;

/**
 * A concrete Locker that simply tracks locks and releases them when
 * operationEnd is called.
 */
public class BasicLocker extends Locker {

    /*
     * A BasicLocker can release all locks, so there is no need to distinguish
     * between read and write locks.
     *
     * ownedLock is used for the first lock obtained, and ownedLockSet is
     * instantiated and used only if more than one lock is obtained.  This is
     * an optimization for the common case where only one lock is held by a
     * non-transactional locker.
     */
    private Lock ownedLock;
    private Set ownedLockSet; 

    /**
     * Creates a BasicLocker.
     */
    public BasicLocker(EnvironmentImpl env)
        throws DatabaseException {

        super(env, false, false);
    }

    /**
     * BasicLockers always have a fixed id, because they are never used for
     * recovery.
     */
    protected long generateId(TxnManager txnManager) {
        return TxnManager.NULL_TXN_ID;
    }

    /**
     * Get a write lock on this LN.
     */
    public LockResult writeLock(LN ln, DatabaseImpl database)
        throws DatabaseException {

        /*
         * If successful, the lock manager will call back to the transaction
         * and add the lock to the lock collection. Done this way because
         * we can't get a handle on the lock without creating another object
         * XXX: bite the bullet, new a holder object, pass it back?
         */
        return new LockResult	
	    (lockManager.lock(ln.getNodeId(), this,
			      LockType.WRITE,
                              lockTimeOutMillis,
                              defaultNoWait),
	     null);
    }

    /**
     * Get a non-blocking write lock on this LN and return LockGrantType.DENIED
     * if it is not granted.
     * This signature is only supported by BasicLocker because it doesn't offer
     * the information needed to rollback a change, such as an abort lsn and a
     * database.
     */
    public LockGrantType nonBlockingWriteLock(LN ln)
        throws DatabaseException {

        long nodeId = ln.getNodeId();
        try {
            return lockManager.lock(nodeId, this, LockType.WRITE, 0, true);
        } catch (LockNotGrantedException e) {
            return LockGrantType.DENIED;
        }
    }

    /**
     * Get the txn that owns the lock on this node. Return null if there's no
     * owning txn found.
     */
    public Locker getWriteOwnerLocker(long nodeId) 
        throws DatabaseException {
   
        return lockManager.getWriteOwnerLocker(new Long(nodeId));
    }

    /**
     * Get the abort lsn for this node in the txn that owns the lock
     * on this node. Return null if there's no owning txn found.
     */
    public DbLsn getOwnerAbortLsn(long nodeId) 
        throws DatabaseException {
   
        Locker ownerTxn = lockManager.getWriteOwnerLocker(new Long(nodeId));
        if (ownerTxn != null) {
            return ownerTxn.getAbortLsn(nodeId);
        }
        return null;
    }

    /**
     * Is not transactional.
     */
    public boolean isTransactional() {
        return false;
    }

    /**
     * Creates a new instance of this txn for the same environment.
     */
    public Locker newInstance()
        throws DatabaseException {

        return new BasicLocker(envImpl);
    }

    /**
     * Returns whether this txn can share locks with the given txn.
     */
    public boolean sharesLocksWith(Locker txn) {
        return false;
    }

    /**
     * Release locks at the end of the transaction.
     */
    public void operationEnd()
        throws DatabaseException {

        operationEnd(true);
    }

    /**
     * Release locks at the end of the transaction.
     */
    public void operationEnd(boolean operationOK)
        throws DatabaseException {

        /*
         * Don't remove locks from txn's lock collection until
         * iteration is done, lest we get a ConcurrentModificationException.
         */
        if (ownedLock != null) {
            lockManager.release(ownedLock, this);
            ownedLock  = null;
        }
        if (ownedLockSet != null) {
            Iterator iter = ownedLockSet.iterator();
            while (iter.hasNext()) {
                Lock l = (Lock) iter.next();
                lockManager.release(l, this);
            }

            // Now clear lock collection.
            ownedLockSet.clear();
        }

        // unload delete info.
        synchronized (this) {
            if ((deleteInfo != null) && (deleteInfo.size() > 0)) {
                envImpl.addToCompressorQueue(deleteInfo.values());
                deleteInfo.clear();
            }
        }
    }

    /**
     * Transfer any MapLN locks to the db handle.
     */
    public void setHandleLockOwner(boolean operationOK,
                                   Database dbHandle,
                                   boolean dbIsClosing)
	throws DatabaseException {


        if (dbHandle != null) { 
            if (!dbIsClosing) {
                transferHandleLockToHandle(dbHandle);
            }
            unregisterHandle(dbHandle);
        }
    }

    /**
     * This txn doesn't store cursors.
     */
    public void registerCursor(CursorImpl cursor) {
    }

    /**
     * This txn doesn't store cursors.
     */
    public void unRegisterCursor(CursorImpl cursor) {
    }

    /*
     * Transactional methods are all no-oped.
     */

    /**
     * @return the abort lsn for this node.
     */
    public DbLsn getAbortLsn(long nodeId)
        throws DatabaseException {

        return null;
    }

    /**
     * @return the abort known deleted state for this node.
     */
    public boolean getAbortKnownDeleted(long nodeId)
        throws DatabaseException {

        return true;
    }

    public void setIsDeletedAtCommit(DatabaseImpl db)
        throws DatabaseException {

        db.deleteAndReleaseINs();
    }

    /**
     * Add a lock to set owned by this transaction. 
     */
    void addLock(long nodeId, Lock lock, LockType type,
                 LockGrantType grantStatus) 
        throws DatabaseException {

        if (ownedLock == lock ||
            (ownedLockSet != null && ownedLockSet.contains(lock))) {
            return; // Already owned
        }
        if (ownedLock == null) {
            ownedLock = lock;
        } else {
            if (ownedLockSet == null) {
                ownedLockSet = new HashSet();
            }
            ownedLockSet.add(lock);
        }
    }
    
    /**
     * Remove a lock from the set owned by this txn.
     */
    void removeLock(long nodeId, Lock lock)
        throws DatabaseException {

        if (lock == ownedLock) {
            ownedLock = null;
        } else if (ownedLockSet != null) {
            ownedLockSet.remove(lock);
        }
    }

    /**
     * Always false for this txn.
     */
    public boolean createdNode(long nodeId) {
        return false;
    }

    /**
     * A lock is being demoted. Move it from the write collection into the
     * read collection.
     */
    void moveWriteToReadLock(long nodeId, Lock lock) {
    }

    /**
     * stats
     */
    public LockStats collectStats(LockStats stats)
        throws DatabaseException {

        if (ownedLock != null) {
            if (ownedLock.isOwnedWriteLock(this)) {
                stats.setNWriteLocks(stats.getNWriteLocks() + 1);
            } else {
                stats.setNReadLocks(stats.getNReadLocks() + 1);
            }
        }
        if (ownedLockSet != null) {
            Iterator iter = ownedLockSet.iterator();
            
            while (iter.hasNext()) {
                Lock l = (Lock) iter.next();
                if (l.isOwnedWriteLock(this)) {
                    stats.setNWriteLocks(stats.getNWriteLocks() + 1);
                } else {
                    stats.setNReadLocks(stats.getNReadLocks() + 1);
                }
            }
        }
        return stats;
    }
}
