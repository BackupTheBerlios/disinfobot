/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: LockManager.java,v 1.1 2004/11/22 18:27:57 kate Exp $
*/

package com.sleepycat.je.txn;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DeadlockException;
import com.sleepycat.je.LockNotGrantedException;
import com.sleepycat.je.LockStats;
import com.sleepycat.je.RunRecoveryException;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.latch.Latch;
import com.sleepycat.je.latch.LatchStats;

/**
 * LockManager manages locks.
 */
public abstract class LockManager {
    protected Latch lockTableLatch;
    private Map lockTable;          // keyed by nodeId
    private EnvironmentImpl env;
    //private Level traceLevel;

    private long nWaits;    // stats: number of time a request blocked 

    public LockManager(EnvironmentImpl env) 
    	throws DatabaseException {
    		
        lockTable = new HashMap();
        lockTableLatch = new Latch("Lock Table", env);
        this.env = env;
        nWaits = 0;
	/*
        traceLevel = Tracer.parseLevel
	    (env, EnvironmentParams.JE_LOGGING_LEVEL_LOCKMGR);
	*/
    }

    /**
     * Attempt to acquire a lock of <i>type</i> on <i>nodeId</i>.  If the lock
     * acquisition would result in a deadlock, throw an exception.<br>
     * If the requested lock is not currently available, block until
     * it is or until timeout milliseconds have elapsed.<br>
     * If a lock of <i>type</i> is already held, return EXISTING.<br>
     * If a WRITE lock is held and a READ lock is requested, return
     * PROMOTION.<br>
     * If a lock request is for a lock that is not currently held, return
     * either NEW or DENIED depending on whether the lock is granted or
     * not.<br>
     *
     * @param nodeId The NodeId to lock.
     *
     * @param locker The Locker to lock this on behalf of.
     *
     * @param type The lock type requested.
     *
     * @param timeout milliseconds to time out after if lock couldn't be
     * obtained.  0 means block indefinitely.  Not used if nonBlockingRequest
     * is true.
     *
     * @param nonBlockingRequest if true, means don't block if lock can't be
     * acquired, and ignore the timeout parameter.
     *
     * @return a LockGrantType indicating whether the request was fulfilled
     * or not.  LockGrantType.NEW means the lock grant was fulfilled and
     * the caller did not previously hold the lock.  PROMOTION means the
     * lock was granted and it was a promotion from READ to WRITE.  EXISTING
     * means the lock was already granted (not a promotion).  DENIED means
     * the lock was not granted either because the timeout passed without
     * acquiring the lock or timeout was -1 and the lock was not immediately
     * available.
     *
     * @throws DeadlockException if acquiring the lock would result in
     * a deadlock.
     *
     * @throws LockNotGrantedException if a non-blocking lock was denied.
     */
    public LockGrantType lock(long nodeId,
                              Locker locker,
                              LockType type,
                              long timeout,
                              boolean nonBlockingRequest)
        throws DeadlockException, LockNotGrantedException, DatabaseException {

        assert timeout >= 0;

        /*
         * Lock on locker before latching the lockTable to avoid having another
         * notifier perform the notify before the waiter is actually waiting.
         */
        synchronized (locker) {

            LockAttemptResult result = attemptLock(nodeId, locker,
                                                   type, nonBlockingRequest);
            /* Got the lock, return. */
            if (result.success) {
                return result.lockGrant;
            }

            assert checkNoLatchesHeld(nonBlockingRequest):
                Latch.countLatchesHeld() +
                " latches held while trying to lock, lock table =" +
                Latch.latchesHeldToString();

            /*
             * We must have gotten WAIT_NEW or WAIT_PROMO from the lock
             * request. We know that this is a blocking request, because
             * if it wasn't, Lock.lock would have returned DENIED. Go wait!
             */
            try {
                boolean doWait = true;
                Long nid = new Long(nodeId);

                /* 
                 * Before blocking, check locker timeout. We need to
                 * check here or lock timeouts will always take
                 * precedence and we'll never actually get any txn
                 * timeouts.
                 */
                if (locker.isTimedOut()) {
                    if (validateOwnership(nid, locker, type, true)) {
                        doWait = false;
                    } else {
                        String errMsg =
                                makeTimeoutMsg("Locker", locker, nodeId, type,
                                               result.lockGrant, 
                                               result.useLock,
                                               locker.getTxnTimeOut(),
                                               locker.getTxnStartMillis(),
                                               System.currentTimeMillis());
                        throw new TxnTimeOutException(errMsg);
                    }
                }

                boolean keepTime = (timeout > 0);
                long startTime = (keepTime ? System.currentTimeMillis() : 0);
                while (doWait) {
                    locker.setWaitingFor(result.useLock);
                    try {
                        locker.wait(timeout);
                    } catch (InterruptedException IE) {
			throw new RunRecoveryException(env, IE);
                    }

                    boolean lockerTimedOut = locker.isTimedOut();
                    long now = System.currentTimeMillis();
                    boolean thisLockTimedOut =
                        (keepTime && (now - startTime > timeout));

                    /*
                     * Re-check for ownership of the lock following
                     * wait.  If we timed out and we don't have
                     * ownership then flush this lock from both the
                     * waiters and owners while under the lock table
                     * latch.  See SR 10103.
                     */ 
                    if (validateOwnership(nid, locker, type, 
                                     (lockerTimedOut | thisLockTimedOut))) {
                        break;
                    } else {
                        if (thisLockTimedOut) {
                            String errMsg =
                                makeTimeoutMsg("Lock", locker, nodeId, type,
                                               result.lockGrant, 
                                               result.useLock,
                                               timeout, startTime, now);
                            throw new LockTimeOutException(errMsg);            
                        } 
                        if (lockerTimedOut) {
                            String errMsg =
                                makeTimeoutMsg("Locker", locker, nodeId, type,
                                               result.lockGrant, 
                                               result.useLock,
                                               locker.getTxnTimeOut(),
                                               locker.getTxnStartMillis(),
                                               now);
                            throw new TxnTimeOutException(errMsg);
                        }
                        
                    }
                }
            } finally {
		locker.setWaitingFor(null);
		if (EnvironmentImpl.getForcedYield()) {
		    Thread.yield();
		}
	    }

            locker.addLock(nodeId, result.useLock, type, result.lockGrant);
            return result.lockGrant;
        }
    }

    abstract protected LockAttemptResult
        attemptLock(long nodeId,
                    Locker locker,
                    LockType type,
                    boolean nonBlockingRequest) 
        throws DatabaseException;
        
    protected LockAttemptResult attemptLockInternal(long nodeId,
                                                    Locker locker,
                                                    LockType type,
                                                    boolean nonBlockingRequest) 
        throws DatabaseException {

        /* Get the target lock. */
        Long nid = new Long(nodeId);
        Lock useLock = (Lock) lockTable.get(nid);
        if (useLock == null) {
            useLock = new Lock(nodeId);
            lockTable.put(nid, useLock);
        }

        /*
         * Attempt to lock. Possible return statuses are
         * NEW, PROMOTED, DENIED, EXISTING, WAIT_NEW, WAIT_PROMO
         */
        LockGrantType lockGrant = useLock.lock(type, locker,
                                               nonBlockingRequest);
        boolean success = false;

        /* Was the attempt successful? */
        if ((lockGrant == LockGrantType.NEW) ||
            (lockGrant == LockGrantType.PROMOTION)) {
            locker.addLock(nodeId, useLock, type, lockGrant);
            success = true;
        } else if (lockGrant == LockGrantType.EXISTING) {
            success = true;
        } else if (lockGrant == LockGrantType.DENIED) {
            throw new LockNotGrantedException
                ("Non-blocking lock was denied.");
        } else {
            nWaits++;
        }
        return new LockAttemptResult(useLock, lockGrant, success);
    }

    /**
     * Create a informative lock or txn timeout message.
     */
    protected abstract String makeTimeoutMsg(String lockOrTxn,
                                             Locker locker,
                                             long nodeId,
                                             LockType type,
                                             LockGrantType grantType,
                                             Lock useLock,
                                             long timeout,
                                             long start,
                                             long now)
	throws DatabaseException;


    /**
     * Do the real work of creating an lock or txn timeout message.
     */
    protected String makeTimeoutMsgInternal(String lockOrTxn,
                                            Locker locker,
                                            long nodeId,
                                            LockType type,
                                            LockGrantType grantType,
                                            Lock useLock,
                                            long timeout,
                                            long start,
                                            long now) {

        /*
         * Because we're accessing parts of the lock, need to have
         * protected access to the lock table because things can be
         * changing out from underneath us.  This is a big hammer to
         * grab for so long while we traverse the graph, but it's only
         * when we have a deadlock and we're creating a debugging
         * message.
         *
         * The alternative would be to handle
         * ConcurrentModificationExceptions and retry until none of
         * them happen.
         */
        StringBuffer sb = new StringBuffer();
        sb.append(lockOrTxn);
        sb.append(" expired. Locker").append(locker);
        sb.append(": waited for lock on node=").append(nodeId);
        sb.append(" type=").append(type);
        sb.append(" grant=").append(grantType);
        sb.append(" timeoutMillis=").append(timeout);
        sb.append(" startTime=").append(start);
        sb.append(" endTime=").append(now);
        sb.append("\nOwners: ").append(useLock.getOwnersClone());
        sb.append("\nWaiters: ").append(useLock.getWaitersListClone()).append("\n");
        StringBuffer deadlockInfo = findDeadlock(useLock, locker);
        if (deadlockInfo != null) {
            sb.append(deadlockInfo);
        }
        return sb.toString();
    }

    /**
     * Release a lock and possibly notify any waiters that they have been
     * granted the lock.
     *
     * @param nodeId The node ID of the lock to release.
     *
     */
    public void release(long nodeId, Locker locker) 
        throws DatabaseException {

        release(nodeId, null, locker, true);
    }

    /**
     * Release a lock and possibly notify any waiters that they have been
     * granted the lock.
     *
     * @param lock The lock to release
     *
     * @return true if the lock is released successfully, false if
     * the lock is not currently being held.
     */
    public void release(Lock lock, Locker locker)
        throws DatabaseException {

        release(-1, lock, locker, false);
    }

    /**
     * Do the work of releasing a lock and notifying any waiters that
     * they have been granted the lock.
     *
     * @param lock The lock to release. If null, use nodeId to find lock
     * @param nodeId The node ID of the lock to release, if lock is null. May
     * not be valid if lock is not null. MUST be valid if removeFromLocker is
     * true
     * @param locker
     * @param removeFromLocker true if we're responsible for 
     */
    private void release(long nodeId,
                         Lock lock,
                         Locker locker,
                         boolean removeFromLocker)
        throws DatabaseException {

	synchronized (locker) {
	    Set newOwners = releaseAndFindNotifyTargets(nodeId,
                                                        lock, 
                                                        locker,
                                                        removeFromLocker);

            if (newOwners != null) {

                /* 
                 * There is a new set of owners.  Notify all of them that
                 * they hold the lock now.
                 */
                Iterator iter = newOwners.iterator();
                
                while (iter.hasNext()) {
                    LockInfo o = (LockInfo) iter.next();
                    Locker pendingOwner = o.getLocker();

                    synchronized (pendingOwner) {
                        pendingOwner.notifyAll();
                    }

                    if (EnvironmentImpl.getForcedYield()) {
                        Thread.yield();
                    }
                }
            }
	}
    }

    /**
     * Release the lock, and return the set of new owners to notify,
     * if any.
     */
    protected abstract Set
        releaseAndFindNotifyTargets(long nodeId,
                                    Lock lock,
                                    Locker locker,
                                    boolean removeFromLocker)
        throws DatabaseException;

    /**
     * Do the real work of releaseAndFindNotifyTargets
     */
    protected Set releaseAndFindNotifyTargetsInternal(long nodeId,
                                                      Lock lock,
                                                      Locker locker,
                                                      boolean removeFromLocker) 
        throws DatabaseException {

        Set newOwners = null;
        boolean notifyNewOwners = false;
        Lock useLock = lock;

        if (useLock == null) {
            useLock = (Lock) lockTable.get(new Long(nodeId));
        }
                
        if (useLock == null) {
            /* Lock doesn't exist. */
            return null; 
        }

        int releaseStatus = useLock.release(locker);
        if (releaseStatus == Lock.RELEASE_NOTIFY) {
            notifyNewOwners = true;
        } else if (releaseStatus == Lock.RELEASE_NOT_OWNER) {
            return null;
        }

        /*
         * If desired, remove it from the locker's bag. Used when
         * we don't need to hang onto the lock after release
         * -- like null txns or locks on deleted LNS
         */
        if (removeFromLocker) {
            assert nodeId != -1;
            locker.removeLock(nodeId, useLock);
        }

        /* If it's not in use at all, remove it from the lock table. */
        if ((useLock.nWaiters() == 0) &&
            (useLock.nOwners() == 0)) {
            lockTable.remove(new Long(useLock.getNodeId()));
        }
        if (notifyNewOwners) {
            newOwners = useLock.getOwnersClone();
        }
        return newOwners;
    }

    /**
     * Transfer ownership a lock from one locker to another locker. We're not
     * sending any notification to the waiters on the lock table, and
     * the past and present owner should be ready for the transfer.
     */
    abstract void transfer(long nodeId,
                           Locker owningLocker,
                           Locker destLocker,
                           boolean demoteToRead) 
        throws DatabaseException;

    /**
     * Do the real work of transfer
     */
    protected void transferInternal(long nodeId,
                                    Locker owningLocker,
                                    Locker destLocker,
                                    boolean demoteToRead) 
        throws DatabaseException {
        
        Lock useLock = (Lock) lockTable.get(new Long(nodeId));
        
        assert useLock != null : "Transfer, lock " + nodeId + " was null";
        if (demoteToRead) {
            useLock.demote(owningLocker);
        }
        LockType lockType = useLock.transfer(owningLocker, destLocker);
        owningLocker.removeLock(nodeId, useLock);
    }

    
    /**
     * Transfer ownership a lock from one locker to a set of other txns,
     * cloning the lock as necessary. This will always be demoted to read,
     * as we can't have multiple locker owners any other way.
     * We're not sending any
     * notification to the waiters on the lock table, and the past and
     * present owners should be ready for the transfer.
     */
    abstract void transferMultiple(long nodeId,
                                   Locker owningLocker,
                                   Locker [] destLockers)
        throws DatabaseException;
            

    /**
     * Do the real work of transferMultiple
     */
    protected void transferMultipleInternal(long nodeId,
                                            Locker owningLocker,
                                            Locker [] destLockers)
        throws DatabaseException {

        Lock useLock = (Lock) lockTable.get(new Long(nodeId));
            
        assert useLock != null : "Transfer, lock " + nodeId + " was null";
        useLock.demote(owningLocker);
        LockType lockType = useLock.transferMultiple(owningLocker,
                                                     destLockers);
        owningLocker.removeLock(nodeId, useLock);
    }

    /**
     * Demote a lock from write to read. Call back to the owning locker to
     * move this to its read collection.
     * @param lock The lock to release. If null, use nodeId to find lock
     * @param locker
     */
    abstract void demote(long nodeId, Locker locker)
        throws DatabaseException;

    /**
     * Do the real work of demote.
     */
    protected void demoteInternal(long nodeId, Locker locker) 
        throws DatabaseException {

        Lock useLock = (Lock) lockTable.get(new Long(nodeId));
        useLock.demote(locker);
        locker.moveWriteToReadLock(nodeId, useLock);
    }

    /**
     * Test the status of the lock on nodeId.  If any transaction
     * holds any lock on it, true is returned.  If no transaction
     * holds a lock on it, false is returned.
     * @param nodeId The NodeId to check.
     * @return true if any transaction holds any lock on the nodeid. false
     * if no lock is held by any transaction.
     */
    abstract boolean isLocked(Long nodeId)
        throws DatabaseException;

    /**
     * Do the real work of isLocked.
     */
    protected boolean isLockedInternal(Long nodeId) {

        Lock entry = (Lock) lockTable.get(nodeId);
        if (entry == null) {
            return false;
        }

        return entry.nOwners() != 0;
    }

    
    /**
     * Return true if this locker owns this a lock of this type on given
     * node.
     */
    abstract boolean isOwner(Long nodeId, Locker locker, LockType type)
        throws DatabaseException;

    /**
     * Do the real work of isOwner.
     */
    protected boolean isOwnerInternal(Long nodeId,
                                      Locker locker,
                                      LockType type) {

        Lock entry = (Lock) lockTable.get(nodeId);
        if (entry == null) {
            return false;
        }

        return entry.isOwner(locker, type);
    }

    /**
     * Return true if this locker is waiting on this lock.
     */
    abstract boolean isWaiter(Long nodeId, Locker locker)
        throws DatabaseException;

    /**
     * Do the real work of isWaiter.
     */
    protected boolean isWaiterInternal(Long nodeId, Locker locker) {

        Lock entry = (Lock) lockTable.get(nodeId);
        if (entry == null) {
            return false;
        }

        return entry.isWaiter(locker);
    }

    /**
     * Return the number of waiters for this lock.
     */
    abstract int nWaiters(Long nodeId)
        throws DatabaseException;

    /**
     * Do the real work of nWaiters.
     */
    protected int nWaitersInternal(Long nodeId) {

        Lock entry = (Lock) lockTable.get(nodeId);
        if (entry == null) {
            return -1;
        }

        return entry.nWaiters();
    }

    /**
     * Return the number of owners of this lock.
     */
    abstract int nOwners(Long nodeId)
        throws DatabaseException;

    /**
     * Do the real work of nWaiters.
     */
    protected int nOwnersInternal(Long nodeId) {

        Lock entry = (Lock) lockTable.get(nodeId);
        if (entry == null) {
            return -1;
        }

        return entry.nOwners();
    }

    /**
     * @return the transaction that owns the write lock for this 
     */
    abstract Locker getWriteOwnerLocker(Long nodeId)
        throws DatabaseException;

    /**
     * Do the real work of getWriteOwnerLocker.
     */
    protected Locker getWriteOwnerLockerInternal(Long nodeId)
        throws DatabaseException {

        Lock lock = (Lock) lockTable.get(nodeId);
        if (lock == null) {
            return null;
        } else if (lock.nOwners() > 1) {
            /* not a write lock */
            return null;
        } else {
            return lock.getWriteOwnerLocker();
        }
    }

    /*
     * Check if we got ownership while we were waiting.
     * If we didn't get ownership, and we timed out, remove this locker
     * from the set of waiters. Do this in a critical section to 
     * prevent any orphaning of the lock -- we must be in a critical section
     * between the time that we check ownership and when we flush any
     * waiters (SR #10103)
     * @return true if you are the owner.
     */
    abstract protected boolean validateOwnership(Long nodeId,
                                                 Locker locker, 
                                                 LockType type,
                                                 boolean flushFromWaiters)
        throws DatabaseException;

    /*
     * Do the real work of validateOwnershipInternal.
     */
    protected boolean validateOwnershipInternal(Long nodeId,
                                                Locker locker, 
                                                LockType type,
                                                boolean flushFromWaiters)
        throws DatabaseException {

        if (isOwnerInternal(nodeId, locker, type)) {
            return true;
        }
            
        if (flushFromWaiters) {
            Lock entry = (Lock) lockTable.get(nodeId);
            if (entry != null) {
                entry.flushWaiter(locker);
            }
        }
        return false;
    }

    /**
     * Statistics
     */
    public LockStats lockStat(StatsConfig config)
        throws DatabaseException {
                
        LockStats stats = new LockStats();
        stats.setNWaits(nWaits);
        if (config.getClear()) {
            nWaits = 0;
        }

        LatchStats latchStats =(LatchStats) lockTableLatch.getLatchStats();
        stats.setLockTableLatchStats(latchStats);

        /* Dump info about the lock table. */
        if (!config.getFast()) {
            dumpLockTable(stats);
        }
        return stats;
    }

    /**
     * Dump the lock table to the lock stats.
     */
    abstract protected void dumpLockTable(LockStats stats) 
        throws DatabaseException;

    /**
     * Do the real work of dumpLockTableInternal.
     */
    protected void dumpLockTableInternal(LockStats stats) {
        stats.setNTotalLocks(lockTable.size());

        Iterator iter = lockTable.values().iterator();
        while (iter.hasNext()) {
            Lock lock = (Lock) iter.next();
            stats.setNWaiters(stats.getNWaiters() +
                              lock.nWaiters());
            stats.setNOwners(stats.getNOwners() +
                             lock.nOwners());

            // Go through all the owners for a lock.
            Iterator ownerIter = lock.getOwnersClone().iterator();
            while (ownerIter.hasNext()) {
                LockInfo info = (LockInfo) ownerIter.next();
                if (info.isReadLock()) {
                    stats.setNReadLocks(stats.getNReadLocks() + 1);
                } else {
                    stats.setNWriteLocks(stats.getNWriteLocks() + 1);
                }
            }
        }

    }

    /**
     * Debugging
     */
    public void dump()
        throws DatabaseException {

        System.out.println(dumpToString());
    }

    public String dumpToString()
        throws DatabaseException {

        StringBuffer sb = new StringBuffer();
        lockTableLatch.acquire();
        Iterator keys = lockTable.keySet().iterator();

        while (keys.hasNext()) {
            Long nid = (Long) keys.next();
            Lock entry = (Lock) lockTable.get(nid);
            sb.append("---- Node Id: ").append(nid).append("----\n");
            sb.append(entry);
            sb.append('\n');
        }
        lockTableLatch.release();
        return sb.toString();
    }

    private boolean checkNoLatchesHeld(boolean nonBlockingRequest) {
        if (nonBlockingRequest) {
            return true; // don't check if it's a non blocking request.
        } else {
            return (Latch.countLatchesHeld() == 0);
        }
    }

    private StringBuffer findDeadlock(Lock lock, Locker rootLocker) {

	Set ownerSet = new HashSet();
	ownerSet.add(rootLocker);
	StringBuffer ret = findDeadlock1(ownerSet, lock, rootLocker);
	if (ret != null) {
	    return ret;
	} else {
	    return null;
	}
    }

    private StringBuffer findDeadlock1(Set ownerSet, Lock lock,
                                       Locker rootLocker) {

	Iterator ownerIter = lock.getOwnersClone().iterator();
	while (ownerIter.hasNext()) {
	    LockInfo info = (LockInfo) ownerIter.next();
	    Locker locker = info.getLocker();
	    Lock waitsFor = locker.getWaitingFor();
	    if (ownerSet.contains(locker) ||
		locker == rootLocker) {
		/* Found a cycle. */
		StringBuffer ret = new StringBuffer();
		ret.append("Transaction ").append(locker.toString());
		ret.append(" owns ").append(lock.getNodeId());
		ret.append(" ").append(info).append("\n");
		ret.append("Transaction ").append(locker.toString());
		ret.append(" waits for ");
		if (waitsFor == null) {
		    ret.append(" nothing");
		} else {
		    ret.append(" node ");
		    ret.append(waitsFor.getNodeId());
		}
		ret.append("\n");
		return ret;
	    }
	    if (waitsFor != null) {
		ownerSet.add(locker);
		StringBuffer sb = findDeadlock1(ownerSet, waitsFor,
                                                rootLocker);
		if (sb != null) {
		    String waitInfo =
			"Transaction " + locker + " waits for node " +
			waitsFor.getNodeId() + "\n";
		    sb.insert(0, waitInfo);
		    return sb;
		}
		ownerSet.remove(locker); // is this necessary?
	    }
	}

	return null;
    }

    /**
     * This is just a struct to hold a multi-value return.
     */
    static class LockAttemptResult {
        boolean success;
        Lock useLock;
        LockGrantType lockGrant;

        LockAttemptResult(Lock useLock,
                          LockGrantType lockGrant,
                          boolean success) {

            this.useLock = useLock;
            this.lockGrant = lockGrant;
            this.success = success;
        }
    }
}
