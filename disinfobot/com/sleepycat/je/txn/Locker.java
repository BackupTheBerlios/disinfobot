/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: Locker.java,v 1.1 2004/11/22 18:27:57 kate Exp $
*/

package com.sleepycat.je.txn;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.LockNotGrantedException;
import com.sleepycat.je.LockStats;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.dbi.CursorImpl;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.BINReference;
import com.sleepycat.je.tree.Key;
import com.sleepycat.je.tree.LN;
import com.sleepycat.je.utilint.DbLsn;

/**
 * Locker instances are JE's route to locking and transactional support.  This
 * class is the abstract base class for BasicLocker, ThreadLocker, Txn and
 * AutoTxn.  Locker instances are in fact only a transaction shell to get to
 * the lock manager, and don't guarantee transactional semantics. Txn and
 * AutoTxn instances are both truely transactional, but have different ending
 * behaviors.
 */
public abstract class Locker {
    private static final String DEBUG_NAME = Locker.class.getName();
    protected EnvironmentImpl envImpl;
    protected LockManager lockManager;

    protected long id;                    // transaction id
    private String name;                  // owning thread's name, debugging
    private boolean dirtyReadDefault;     // true if dirty reads are default

    /* Timeouts */
    protected boolean defaultNoWait;      // true for non-blocking
    protected long lockTimeOutMillis;     // timeout period for lock, in ms
    private long txnTimeOutMillis;        // timeout period for txns, in ms
    private long txnStartMillis;          // for txn timeout determination

    private Lock waitingFor;              // The lock that this txn is
                                          // waiting for.

    /*
     * DeleteInfo refers to BINReferences that should be sent to the
     * INCompressor for asynchronous compressing after the transaction ends.
     */
    protected Map deleteInfo;             

    /*
     * To support handle lock transfers, each txn keeps maps handle locks to
     * database handles. This is maintained as a map where the key is the
     * handle lock id and the value is a set of database handles that
     * correspond to that handle lock. This is a 1 - many relationship because
     * a single handle lock can cover multiple database handles opened by the
     * same transaction.
     */
    protected Map handleLockToHandleMap; // 1-many, used for commits
    protected Map handleToHandleLockMap; // 1-1, used for aborts

    /**
     * Create a locker id. This constructor is called very often, so it should
     * be as streamlined as possible.
     * 
     * @param lockManager lock manager for this environment
     * @param dirtyReadDefault if true, this transaction does dirty reads
     * by default
     * @param noWait if true, non-blocking lock requests are used.
     */
    public Locker(EnvironmentImpl envImpl,
                  boolean dirtyReadDefault,
                  boolean noWait) 
        throws DatabaseException {

        TxnManager txnManager = envImpl.getTxnManager();
        this.id = generateId(txnManager);
        this.envImpl = envImpl;
        lockManager = txnManager.getLockManager();
        this.dirtyReadDefault = dirtyReadDefault;
	this.waitingFor = null;

        /* get the default lock timeout. */
        defaultNoWait = noWait;
        lockTimeOutMillis = envImpl.getLockTimeout();

        /*
         * Check the default txn timeout. If non-zero, remember the txn start
         * time.
         */
        txnTimeOutMillis = envImpl.getTxnTimeout();

        if (txnTimeOutMillis != 0) {
            txnStartMillis = System.currentTimeMillis();
        } else {
            txnStartMillis = 0;
        }

        /* Initialized deleteInfo when needed. */
        name = Thread.currentThread().getName();

        /* 
         * Don't initialize handle lock maps until required, they're seldom
         * used.
         */
    }

    /**
     * For reading from the log.
     */
    Locker() {
    }

    /**
     * A Locker has to generate its next id. Some subtypes, like BasicLocker,
     * have a single id for all instances because they are never used for
     * recovery. Other subtypes ask the txn manager for an id.
     */
    protected abstract long generateId(TxnManager txnManager);

    /**
     * @return the transaction's id.
     */
    public long getId() {
        return id;
    }

    /**
     * Get the lock timeout period for this transaction, in milliseconds
     */
    public synchronized long getLockTimeout() {
        return lockTimeOutMillis;
    }

    /**
     * Set the lock timeout period for any locks in this transaction,
     * in milliseconds.
     */
    public synchronized void setLockTimeout(long timeOutMillis) {
        lockTimeOutMillis = timeOutMillis;
    }

    /**
     * Set the timeout period for this transaction, in milliseconds.
     */
    public synchronized void setTxnTimeout(long timeOutMillis) {
        txnTimeOutMillis = timeOutMillis;
        txnStartMillis = System.currentTimeMillis();
    }

    /**
     * @return true if transaction was created with dirty reads as a default.
     */
    public boolean isDirtyReadDefault() {
        return dirtyReadDefault;
    }

    Lock getWaitingFor() {
	return waitingFor;
    }

    void setWaitingFor(Lock lock) {
	waitingFor = lock;
    }

    /*
     * Obtain and release locks.
     */ 

    /**
     * Get a read lock on this LN.
     */
    public LockGrantType readLock(LN ln)
        throws DatabaseException {
  
	long timeout;
	synchronized (this) {
	    timeout = lockTimeOutMillis;
	}

        /*
         * If successful, the lock manager will call back to the transaction
         * and add the lock to the lock collection.  Don't call readLock(LN,
         * long), because since ThreadLocker calls super.readLock, we'd bounce
         * into four readLock calls.
         */
        long nodeId = ln.getNodeId();
        return lockManager.lock(nodeId, this, LockType.READ,
                                timeout, defaultNoWait);
    }

    /**
     * Get a non-blocking read lock on this LN and return LockGrantType.DENIED
     * if it is not granted.
     */
    public LockGrantType nonBlockingReadLock(LN ln)
        throws DatabaseException {

        long nodeId = ln.getNodeId();
  
        /*
         * If successful, the lock manager will call back to the transaction
         * and add the lock to the lock collection. Done this way because we
         * can't get a handle on the lock without creating another object XXX:
         * bite the bullet, new a holder object, pass it back?
         */
        try {
            return lockManager.lock(nodeId, this, LockType.READ, 0, true);
        } catch (LockNotGrantedException e) {
            return LockGrantType.DENIED;
        }
    }

    /**
     * Get a write lock on this LN.  Caller is responsible for calling
     * LockResult.setAbortLsn() on the returned value.
     */
    public abstract LockResult writeLock(LN ln, DatabaseImpl database)
        throws DatabaseException; 

    /**
     * Release the lock on this LN and remove from the transaction's owning
     * set.
     */
    public void releaseLock(LN ln)
        throws DatabaseException {

        /*
         * If successful, the lock manager will call back to the transaction
         * and remove the lock from the lock collection. Done this way because
         * we can't get a handle on the lock without creating another object
         * XXX: bite the bullet, new a holder object, pass it back?
         */
        lockManager.release(ln.getNodeId(), this);
    }

    /**
     * Revert this lock from a write lock to a read lock.
     */
    public void demoteLock(long nodeId)
        throws DatabaseException {

        /*
         * If successful, the lock manager will call back to the transaction
         * and adjust the location of the lock in the lock collection.
         */
        lockManager.demote(nodeId, this);
    }

    /**
     * Returns whether this locker is transactional.
     */
    public abstract boolean isTransactional();

    /**
     * Creates a new instance of this txn for the same environment.
     * Currently this is only used for ThreadLocker.
     */
    public abstract Locker newInstance()
        throws DatabaseException;

    /**
     * Returns whether this txn can share locks with the given txn.  This is
     * the true when both txns are non-transactional lockers for the same
     * thread.
     */
    public abstract boolean sharesLocksWith(Locker txn);

    /**
     * The equivalent of calling operationEnd(true)..
     */
    public abstract void operationEnd()
        throws DatabaseException;

    /**
     * Different types of transactions do different things when the operation
     * ends. Txns do nothing, AutoTxns commit or abort, and BasicLockers and
     * ThreadLockers just release locks.
     *
     * @param operationOK is whether the operation succeeded, since
     * that may impact ending behavior. (i.e for AutoTxn)
     */
    public abstract void operationEnd(boolean operationOK)
        throws DatabaseException;

    /**
     * We're at the end of an operation. Move this handle lock to the
     * appropriate owner.
     */
    public abstract void setHandleLockOwner(boolean operationOK,
                                            Database dbHandle,
                                            boolean dbIsClosing)
        throws DatabaseException;

    /**
     * A SUCCESS status equals operationOk.
     */
    public void operationEnd(OperationStatus status) 
        throws DatabaseException {

        operationEnd(status == OperationStatus.SUCCESS);
    }

    /**
     * Tell this transaction about a cursor.
     */
    public abstract void registerCursor(CursorImpl cursor)
        throws DatabaseException;

    /**
     * Remove a cursor from this txn.
     */
    public abstract void unRegisterCursor(CursorImpl cursor)
        throws DatabaseException;

    /*
     * Transactional support
     */

    /**
     * @return the abort lsn for this node
     */
    public abstract DbLsn getAbortLsn(long nodeId)
        throws DatabaseException;

    /**
     * @return the abort lsn for this node
     */
    public abstract boolean getAbortKnownDeleted(long nodeId)
        throws DatabaseException;

    /**
     * Note a database being deleted.
     */
    abstract public void setIsDeletedAtCommit(DatabaseImpl db)
        throws DatabaseException;

    /**
     * Add delete information, to be added to the inCompressor queue
     * when the transaction ends.
     */
    public void addDeleteInfo(BIN bin, Key deletedKey)
        throws DatabaseException {

        synchronized (this) {
            /* Maintain only one binRef per node. */
            if (deleteInfo == null) {
                deleteInfo = new HashMap();
            }
            Long nodeId = new Long(bin.getNodeId());
            BINReference binRef = (BINReference) deleteInfo.get(nodeId);
            if (binRef == null) {
                binRef = bin.createReference();
                deleteInfo.put(nodeId, binRef);  
            }
            binRef.addDeletedKey(deletedKey);
        }
    }
    
    /*
     * Manage locks owned by this transaction. Note that transactions that will
     * be multithreaded must override these methods and provide synchronized
     * implementations.
     */

    /**
     * Add a lock to set owned by this transaction.
     */
    abstract void addLock(long nodeId,
                          Lock lock,
                          LockType type,
                          LockGrantType grantStatus)
        throws DatabaseException;

    /**
     * @return true if this transaction created this node,
     * for a operation with transactional semantics.
     */
    public abstract boolean createdNode(long nodeId)
        throws DatabaseException;

    /**
     * Remove the lock from the set owned by this transaction. If specified to
     * LockManager.release, the lock manager will call this when its releasing
     * a lock.
     */
    abstract void removeLock(long nodeId, Lock lock)
        throws DatabaseException;

    /**
     * A lock is being demoted. Move it from the write collection into the read
     * collection.
     */
    abstract void moveWriteToReadLock(long nodeId, Lock lock)
        throws DatabaseException;

    /**
     * Get lock count, for per transaction lock stats, for internal debugging.
     */
    public abstract LockStats collectStats(LockStats stats)
        throws DatabaseException;

    /* 
     * Check txn timeout, if set. Called by the lock manager when blocking on a
     * lock.
     */
    boolean isTimedOut()
        throws DatabaseException {

        if (txnStartMillis != 0) {
            long diff = System.currentTimeMillis() - txnStartMillis;
            if (diff > txnTimeOutMillis) {
                return true;
            } 
        }
        return false;
    }

    long getTxnTimeOut() {
        return txnTimeOutMillis;
    }

    long getTxnStartMillis() {
        return txnStartMillis;
    }

    /**
     * Remove this Database from the protected Database handle set
     */
    void unregisterHandle(Database dbHandle) {

    	/* 
    	 * handleToHandleLockMap may be null if the db handle was never really
    	 * added. This might be the case because of an unregisterHandle that
    	 * comes from a finally clause, where the db handle was never
    	 * successfully opened.
    	 */
    	if (handleToHandleLockMap != null) {
            handleToHandleLockMap.remove(dbHandle);
    	}
    }

    /**
     * Remember how handle locks and handles match up.
     */
    public void addToHandleMaps(Long handleLockId, 
                               Database databaseHandle) {
        // TODO: mutex after measurement
        Set dbHandleSet = null;
        if (handleLockToHandleMap == null) {

            /* 
	     * We do lazy initialization of the maps, since they're used
             * infrequently.
             */
            handleLockToHandleMap = new Hashtable();
            handleToHandleLockMap = new Hashtable();
        } else {
            dbHandleSet = (Set) handleLockToHandleMap.get(handleLockId);
        }

        if (dbHandleSet == null) {
            dbHandleSet = new HashSet();
            handleLockToHandleMap.put(handleLockId, dbHandleSet);
        }

        /* Map handle lockIds -> 1 or more database handles. */
        dbHandleSet.add(databaseHandle);
        /* Map database handles -> handle lock id */
        handleToHandleLockMap.put(databaseHandle, handleLockId);
    }

    /**
     * @return true if this txn is willing to give up the handle lock to
     * another txn before this txn ends.
     */
    public boolean isHandleLockTransferrable() {
        return true;
    }

    /**
     * The currentTxn passes responsiblity for this db handle lock to a txn
     * owned by the Database object.
     */
    void transferHandleLockToHandle(Database dbHandle)
        throws DatabaseException {

        /* 
         * Transfer responsiblity for this db lock from this txn to a new
         * protector.
         */
        Locker holderTxn = new BasicLocker(envImpl);
        transferHandleLock(dbHandle, holderTxn, true );
    }

    /**
     * 
     */
    public void transferHandleLock(Database dbHandle,
                                   Locker destLocker,
                                   boolean demoteToRead)
        throws DatabaseException {

        /* 
         * Transfer responsiblity for dbHandle's handle lock from this txn to
         * destLocker. If the dbHandle's databaseImpl is null, this handle
         * wasn't opened successfully.
         */
        if (DbInternal.dbGetDatabaseImpl(dbHandle) != null) {
            Long handleLockId = (Long) handleToHandleLockMap.get(dbHandle);
            if (handleLockId != null) {
                /* We have a handle lock for this db. */
                long nodeId = handleLockId.longValue();
                
                /* If needed, remember that we onced owned this lock. */
                rememberHandleWriteLock(handleLockId);

                /* Move this lock to the destination txn. */
                lockManager.transfer(nodeId, this, destLocker, demoteToRead);

                /* 
                 * Make the destination txn remember that it now owns this
                 * handle lock.
                 */
                destLocker.addToHandleMaps(handleLockId, dbHandle);

                /* Take this out of the handle lock map. */
                Set dbHandleSet = (Set)
		    handleLockToHandleMap.get(handleLockId);
                Iterator iter = dbHandleSet.iterator();
                while (iter.hasNext()) {
                    if (((Database) iter.next()) == dbHandle) {
                        iter.remove();
                        break;
                    }
                }
                if (dbHandleSet.size() == 0) {
                    handleLockToHandleMap.remove(handleLockId);
                }
                
                /* 
                 * This Database must remember what txn owns it's handle lock.
                 */
                DbInternal.dbSetHandleLocker(dbHandle, destLocker);
            }
        }
    }

    /**
     * If necessary, remember that this txn once owned a handle lock.  Done to
     * make commit optimizations work correctly.
     */
    protected void rememberHandleWriteLock(Long lockId) {
        /* by default, nothing to do. */
    }
    
    /*
     * Helpers
     */
    public String toString() {
        return Long.toString(id) + "_" + name;
    }

    /**
     * Dump lock table, for debugging
     */
    public void dumpLockTable() 
        throws DatabaseException {

        lockManager.dump();
    }

    public String getName() {
        return name;
    }
}
