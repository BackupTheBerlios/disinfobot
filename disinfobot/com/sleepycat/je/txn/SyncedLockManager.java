/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: SyncedLockManager.java,v 1.1 2004/11/22 18:27:57 kate Exp $
*/

package com.sleepycat.je.txn;

import java.util.Set;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.LockStats;
import com.sleepycat.je.dbi.EnvironmentImpl;

/**
 * SyncedLockManager uses the synchronized keyword to implement its
 * critical sections.
 */
public class SyncedLockManager extends LockManager {

    public SyncedLockManager(EnvironmentImpl envImpl) 
    	throws DatabaseException {
        super(envImpl);
    }

    /**
     * @see LockManager#attemptLock
     */
    protected LockAttemptResult attemptLock(long nodeId,
                                            Locker locker,
                                            LockType type,
                                            boolean nonBlockingRequest) 
        throws DatabaseException {
        
        synchronized(lockTableLatch) {
            return attemptLockInternal(nodeId, locker, type, 
                                       nonBlockingRequest);
        }

    }
        
    /**
     * @see LockManager#makeTimeoutMsg
     */
    protected String makeTimeoutMsg(String lockOrTxn,
                                    Locker locker,
                                    long nodeId,
                                    LockType type,
                                    LockGrantType grantType,
                                    Lock useLock,
                                    long timeout,
                                    long start,
                                    long now) {

        synchronized(lockTableLatch) {
            return makeTimeoutMsgInternal(lockOrTxn, locker, nodeId, type,
                                          grantType, useLock, timeout,
                                          start, now);
        }
    }

    /**
     * @see LockManager#releaseAndNotifyTargets
     */
    protected Set releaseAndFindNotifyTargets(long nodeId,
                                              Lock lock,
                                              Locker locker,
                                              boolean removeFromLocker) 
        throws DatabaseException {

        synchronized(lockTableLatch) {
            return releaseAndFindNotifyTargetsInternal(nodeId, lock,
                                                       locker,
                                                       removeFromLocker);
        }
    }

    /**
     * @see LockManager#transfer
     */
    void transfer(long nodeId,
                  Locker owningLocker,
                  Locker destLocker,
                  boolean demoteToRead) 
        throws DatabaseException {

        synchronized(lockTableLatch) {
            transferInternal(nodeId, owningLocker, destLocker, demoteToRead);
        }
    }

    /**
     * @see LockManager#transferMultiple
     */
    void transferMultiple(long nodeId,
                          Locker owningLocker,
                          Locker [] destLockers)
        throws DatabaseException {

        synchronized(lockTableLatch) {
            transferMultipleInternal(nodeId, owningLocker, destLockers);
        }
    }

    /**
     * @see LockManager#demote
     */
    void demote(long nodeId, Locker locker)
        throws DatabaseException {
        
        synchronized(lockTableLatch) {
            demoteInternal(nodeId, locker);
        }
    }

    /**
     * @see LockManager#isLocked
     */
    boolean isLocked(Long nodeId) {

        synchronized(lockTableLatch) {
            return isLockedInternal(nodeId);
        }
    }

    /**
     * @see LockManager#isOwner
     */
    boolean isOwner(Long nodeId, Locker locker, LockType type) {

        synchronized(lockTableLatch) {
            return isOwnerInternal(nodeId, locker, type);
        }
    }

    /**
     * @see LockManager#isWaiter
     */
    boolean isWaiter(Long nodeId, Locker locker) {
        
        synchronized(lockTableLatch) {
            return isWaiterInternal(nodeId, locker);
        }
    }

    /**
     * @see LockManager#nWaiters
     */
    int nWaiters(Long nodeId) {

          synchronized(lockTableLatch) {
            return nWaitersInternal(nodeId);
        }
    }

    /**
     * @see LockManager#nOwners
     */
    int nOwners(Long nodeId) {

        synchronized(lockTableLatch) {
            return nOwnersInternal(nodeId);
        }
    }

    /**
     * @see LockManager#getWriterOwnerLocker
     */
    Locker getWriteOwnerLocker(Long nodeId)
        throws DatabaseException {

        synchronized(lockTableLatch) {
            return getWriteOwnerLockerInternal(nodeId);
        }
    }

    /**
     * @see LockManager#validateOwnership
     */
    protected boolean validateOwnership(Long nodeId,
                                        Locker locker, 
                                        LockType type,
                                        boolean flushFromWaiters)
        throws DatabaseException {


        synchronized(lockTableLatch) {
            return validateOwnershipInternal(nodeId, locker,
                                             type, flushFromWaiters);
        }
    }

    /**
     * @see LockManager#dumpLockTable
     */
    protected void dumpLockTable(LockStats stats) 
        throws DatabaseException {
        
        synchronized(lockTableLatch) {
            dumpLockTableInternal(stats);
        }
    }
}
