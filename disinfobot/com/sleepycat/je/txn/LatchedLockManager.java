/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: LatchedLockManager.java,v 1.1 2004/11/22 18:27:57 kate Exp $
*/

package com.sleepycat.je.txn;

import java.util.Set;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.LockStats;
import com.sleepycat.je.dbi.EnvironmentImpl;

/**
 * LatchedLockManager uses latches to implement its
 * critical sections.
 */
public class LatchedLockManager extends LockManager {

    public LatchedLockManager(EnvironmentImpl envImpl) 
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
        
        lockTableLatch.acquire();
        try {
            return attemptLockInternal(nodeId, locker, type, 
                                       nonBlockingRequest);
        } finally {
            lockTableLatch.release();
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
                                    long now) 
        throws DatabaseException {

        lockTableLatch.acquire();
        try {
            return makeTimeoutMsgInternal(lockOrTxn, locker,
                                          nodeId, type, grantType,
                                          useLock, timeout, start, now);
        } finally {
            lockTableLatch.release();
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

        lockTableLatch.acquire(); 
        try {
            return releaseAndFindNotifyTargetsInternal(nodeId, lock,
                                                       locker,
                                                       removeFromLocker);
        } finally {
            lockTableLatch.release();
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

        lockTableLatch.acquire(); 
        try {
            transferInternal(nodeId, owningLocker, destLocker, demoteToRead);
        } finally {
            lockTableLatch.release();
        }
    }
    /**
     * @see LockManager#transferMultiple
     */
    void transferMultiple(long nodeId,
                          Locker owningLocker,
                          Locker [] destLockers)
        throws DatabaseException {

        lockTableLatch.acquire();
        try {
            transferMultipleInternal(nodeId, owningLocker, destLockers);
        } finally {
            lockTableLatch.release();
        }
    }

    /**
     * @see LockManager#demote
     */
    void demote(long nodeId, Locker locker)
        throws DatabaseException {
        
        lockTableLatch.acquire();
        try {
            demoteInternal(nodeId, locker);
        } finally {
            lockTableLatch.release();
        }
    }

    /**
     * @see LockManager#isLocked
     */
    boolean isLocked(Long nodeId) 
        throws DatabaseException {

        lockTableLatch.acquire();
        try {
            return isLockedInternal(nodeId);
        } finally {
            lockTableLatch.release();
        }
    }

    /**
     * @see LockManager#isOwner
     */
    boolean isOwner(Long nodeId, Locker locker, LockType type) 
        throws DatabaseException {

        lockTableLatch.acquire();
        try {
            return isOwnerInternal(nodeId, locker, type);
        } finally {
            lockTableLatch.release();
        }
    }

    /**
     * @see LockManager#isWaiter
     */
    boolean isWaiter(Long nodeId, Locker locker) 
        throws DatabaseException {
        
        lockTableLatch.acquire();
        try {
            return isWaiterInternal(nodeId, locker);
        } finally {
            lockTableLatch.release();
        }
    }

    /**
     * @see LockManager#nWaiters
     */
    int nWaiters(Long nodeId) 
        throws DatabaseException {

        lockTableLatch.acquire();
        try {
            return nWaitersInternal(nodeId);
        } finally {
            lockTableLatch.release();
        }
    }

    /**
     * @see LockManager#nOwners
     */
    int nOwners(Long nodeId) 
        throws DatabaseException {

        lockTableLatch.acquire();
        try {
            return nOwnersInternal(nodeId);
        } finally {
            lockTableLatch.release();
        }
    }

    /**
     * @see LockManager#getWriterOwnerLocker
     */
    Locker getWriteOwnerLocker(Long nodeId)
        throws DatabaseException {

        lockTableLatch.acquire();
        try {
            return getWriteOwnerLockerInternal(nodeId);
        } finally {
            lockTableLatch.release();
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

        lockTableLatch.acquire();
        try {
            return validateOwnershipInternal(nodeId, locker,
                                             type, flushFromWaiters);
        } finally {
            lockTableLatch.release();
        }
    }

    /**
     * @see LockManager#dumpLockTable
     */
    protected void dumpLockTable(LockStats stats) 
        throws DatabaseException {
        
        lockTableLatch.acquire();
        try {
            dumpLockTableInternal(stats);
        } finally {
            lockTableLatch.release();
        }
    }
}
