/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: Lock.java,v 1.1 2004/11/22 18:27:57 kate Exp $
*/

package com.sleepycat.je.txn;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.sleepycat.je.DatabaseException;

/**
 * A Lock embodies the lock state of a NodeId.  It includes a set of
 * owners and a list of waiters.
 */
public class Lock {

    static final int RELEASE_NOT_OWNER = 0;
    static final int RELEASE_NOTIFY = 1;
    static final int RELEASE_NO_NOTIFY = 2;

    /**
     * A single locker always appears only once in the logical set of owners.
     * The owners set is always in one of the following states.
     * 1- Empty
     * 2- A single writer
     * 3- One or more readers
     * 4- Multiple writers or a mix of readers and writers, all for
     * txns which share locks (all ThreadLocker instances for the same
     * thread)
     *
     * Both ownerSet and waiterList are a collection of LockInfo.
     * Since the common case is that there is only one owner or
     * waiter, we have added an optimization to avoid the cost of
     * collections.  FirstOwner and firstWaiter are used for the first
     * owner or waiter of the lock, and the corresponding collection
     * is instantiated and used only if more owners arrive.
     */
    private LockInfo firstOwner;    
    private Set ownerSet; 
    private LockInfo firstWaiter;    
    private List waiterList; 
    private long nodeId;

    /**
     * Create a Lock.
     */
    Lock(long nodeId) {
        this.nodeId = nodeId;
    }

    long getNodeId() {
        return nodeId;
    }

    /**
     * The first waiter goes into the firstWaiter member variable.
     * Once the waiterList is made, all appended waiters go into
     * waiterList, even after the firstWaiter goes away and leaves
     * that field null, so as to leave the list ordered.
     */
    private void addWaiterToEndOfList(LockInfo waiter) {
        /* Careful: order important! */
        if (waiterList == null) {
            if (firstWaiter == null) {
                firstWaiter = waiter;
            } else {
                waiterList = new ArrayList();
                waiterList.add(waiter);
            }
        } else {
            waiterList.add(waiter);
        }
    }

    /**
     * Add this waiter to the front of the list.
     */
    private void addWaiterToHeadOfList(LockInfo waiter) {
        /* Shuffle the current first waiter down a slot. */
        if (firstWaiter != null) {
            if (waiterList == null) {
                waiterList = new ArrayList();
            }
            waiterList.add(0, firstWaiter);
        }

        firstWaiter = waiter;
    }

    /* Get a list of waiters for debugging and error messages. */
    List getWaitersListClone() {
        List dumpWaiters = new ArrayList();
        if (firstWaiter != null) {
            dumpWaiters.add(firstWaiter);
        } 

        if (waiterList != null) {
            dumpWaiters.addAll(waiterList);
        }

        return dumpWaiters;
    }

    /** 
     * Remove this locker from the waiter list.
     */
    void flushWaiter(Locker locker) {
        if ((firstWaiter != null) && (firstWaiter.getLocker() == locker)) {
            firstWaiter = null;
        } else if (waiterList != null) { 
            Iterator iter = waiterList.iterator();
            while (iter.hasNext()) {
                LockInfo info = (LockInfo) iter.next();
                if (info.getLocker() == locker) {
                    iter.remove();
                    return;
                }
            }
        }
    }

    private void addOwner(LockInfo newLock) {
        if (firstOwner == null) {
            firstOwner = newLock;
        } else {
            if (ownerSet == null) {
                ownerSet = new HashSet();
            }
            ownerSet.add(newLock);
        }
    }

    /*
     * Get a new Set of the owners.
     */
    Set getOwnersClone() {
        Set owners;
        if (ownerSet != null) {
            owners = new HashSet(ownerSet);
        } else {
            owners = new HashSet();
        }
        if (firstOwner != null) {
            owners.add(firstOwner);
        }
        return owners;
    }

    /**
     * Remove this LockInfo from the owner set.
     */
    private boolean flushOwner(LockInfo oldOwner) {
        boolean removed = false;
        if (oldOwner != null) {
            if (firstOwner == oldOwner) {
                firstOwner = null;
                return true;
            }

            if (ownerSet != null) {
                removed = ownerSet.remove(oldOwner);
            }
        }
        return removed;
    }

    /**
     * Remove this locker from the owner set.
     */
    private LockInfo flushOwner(Locker locker) {

        if ((firstOwner != null) && (firstOwner.getLocker() == locker)) {
            LockInfo flushed = firstOwner;
            firstOwner = null;
            return flushed;
        } else if (ownerSet != null) {
            Iterator iter = ownerSet.iterator();
            while (iter.hasNext()) {
                LockInfo o = (LockInfo) iter.next();
                if (o.getLocker() == locker) {
                    iter.remove();
                    return o;
                }
            }
        }
        return null;
    }


    /*
     * Return true if the lock was acquired, false if we are placed on the
     * waiters queue.
     *
     * Assumes we hold the lockTableLatch when entering this method.
     */
    LockGrantType lock(LockType requestType,
                       Locker locker,
                       boolean nonBlockingRequest)
        throws DatabaseException {

        assert validateRequest(locker); // intentional side effect

        LockInfo newLock = new LockInfo(locker, requestType);

        /* If no one owns this right now, just grab it. */
        int numOwners = nOwners();
        if (numOwners == 0) {
            addOwner(newLock);
            return LockGrantType.NEW;
        }

        boolean allOwnersAreReaders = true;
        boolean allOwnersShareLocks = true;
        LockInfo lockToPromote = null;

        /* 
         * Iterate through the current owners. See if there is a
         * current owner who has to be promoted from read to
         * write. Also track whether all owners are readers and
         * sharers.
         */
        LockInfo owner = null;
        Iterator iter = null;
        
        if (ownerSet != null) {
            iter = ownerSet.iterator();
        }

        if (firstOwner != null) {
            owner = firstOwner;
        } else if ((iter !=null) && (iter.hasNext())) {
            owner = (LockInfo) iter.next();
        }

        while (owner != null) {
            if (owner.getLocker() == locker) {
                /* Requestor currently holds this lock. */
                if (owner.isReadLock()) {
                    if (requestType == LockType.WRITE) {
                        /* Requested WRITE and currently holds READ. */
                        assert lockToPromote == null;
                        lockToPromote = owner;
                    } else {
                        /* Requested READ and currently holds READ. */
                        return LockGrantType.EXISTING;
                    }
                } else {
                    /* Requested READ or WRITE and currently holds WRITE. */
                    return LockGrantType.EXISTING;
                }
            }
            if (allOwnersAreReaders && !owner.isReadLock()) {
                allOwnersAreReaders = false;
            }
            if (allOwnersShareLocks &&
                !locker.sharesLocksWith(owner.getLocker())) {
                allOwnersShareLocks = false;
            }

            /* Move on to the next owner. */
            if ((iter != null) && (iter.hasNext())) {
                owner = (LockInfo) iter.next();
            } else {
                owner = null;
            }
        }

        if (lockToPromote != null) {
            /* Requestor holds a read lock and is requesting a write lock. */
            if (numOwners == 1 || allOwnersShareLocks) {
                /*
                 * If requestor is the only owner, or if all owners share
                 * locks with the requestor, grant the promotion.
                 */
                lockToPromote.setLockType(requestType);
                return LockGrantType.PROMOTION;
            } else {
                /* Cannot grant the promotion at this time. */
                if (!nonBlockingRequest) {
                    addWaiterToHeadOfList(newLock);
                    return LockGrantType.WAIT_PROMOTION;
                } else {
                    return LockGrantType.DENIED;
                }
            }
        }

        /* 
         * The requestor doesn't hold this lock. 
         *
         * If all owners and the requestor can share locks, grant the lock
         * irrespective of its type.  This is the case where all txns are
         * non-transactional lockers for the same thread.  Note that this may
         * cause a mix of readers and writers in the owner list.
         *
         * Or, if we're requesting a read lock and there are only read owners
         * and there are no waiters (they would just be waiting for write),
         * then we can grant the read lock.
         */
        if (allOwnersShareLocks ||
            (requestType == LockType.READ &&
             allOwnersAreReaders &&
             (nWaiters() == 0))) {

            addOwner(newLock);
            return LockGrantType.NEW;
        }

        /* Lock cannot be granted at this time. */
        if (!nonBlockingRequest) {
            addWaiterToEndOfList(newLock);
            return LockGrantType.WAIT_NEW;
        } else {
            return LockGrantType.DENIED;
        }
    }

    /**
     * Returns the owner LockInfo for a locker, or null if locker is
     * not an owner.
     */
    private LockInfo getOwnerLockInfo(Locker locker) {
        if ((firstOwner != null) && (firstOwner.getLocker() == locker)) {
            return firstOwner;
        }

        if (ownerSet != null) {
            Iterator iter = ownerSet.iterator();
            while (iter.hasNext()) {
                LockInfo o = (LockInfo) iter.next();
                if (o.getLocker() == locker) {
                    return o;
                }
            }
        }

        return null;
    }

    /*
     * @return true if locker is an owner of this Lock for lockType,
     * false otherwise.
     */
    boolean isOwner(Locker locker, LockType lockType) {
        LockInfo o = getOwnerLockInfo(locker);
        if (o != null) {
            LockType ownedLockType = o.getLockType();
            if (lockType == null ||
                ownedLockType == lockType ||
                (ownedLockType == LockType.WRITE &&
                 lockType == LockType.READ)) {
                return true;
            }
        }
        return false;
    }

    /**
     * @return true if locker is an owner of this Lock and this is a write
     * lock.
     */
    boolean isOwnedWriteLock(Locker locker) {
        LockInfo o = getOwnerLockInfo(locker);
        return o != null && o.isWriteLock();
    }

    /**
     * Return true if locker is a waiter on this Lock.
     */
    boolean isWaiter(Locker locker) {

        if (firstWaiter != null) {
            if (firstWaiter.getLocker() == locker) {
                return true;
            }
        }

        if (waiterList != null) {
            Iterator iter = waiterList.iterator();
            while (iter.hasNext()) {
                LockInfo info = (LockInfo) iter.next();
                if (info.getLocker() == locker) {
                    return true;
                }
            }
        }
        return false;
    }

    int nWaiters() {
        int count = 0;
        if (firstWaiter != null) {
            count++;
        }
        if (waiterList != null) {
            count += waiterList.size();
        }
        return count;
    }

    int nOwners() {
        int count = 0;
        if (firstOwner != null) {
            count++;
        }

        if (ownerSet != null) {
            count += ownerSet.size();
        }
        return count;
    }

    /**
     * Release a lock and move the next waiter(s) to the owners.
     * @return
     * RELEASE_NOT_OWNER if we were not the owner,
     * RELEASE_NOTIFY if owners should be notified after releasing,
     * RELEASE_NO_NOTIFY if no notification is required.
     */
    int release(Locker locker) {

        LockInfo removedLock = flushOwner(locker);
        if (removedLock == null) {
            return RELEASE_NOT_OWNER;
        }

        if (nWaiters() == 0) {
            /* No more waiters, so no one to notify. */
            return RELEASE_NO_NOTIFY;
        }

        /* Determine some things about the remaining owners. */
        boolean allOwnersShareLocks = true;
        boolean allOwnersAreReaders = true;
        Locker anyOwnerLocker = null;

        LockInfo owner = null;
        Iterator iter = null;
        
        if (ownerSet != null) {
            iter = ownerSet.iterator();
        }

        if (firstOwner != null) {
            owner = firstOwner;
        } else if ((iter !=null) && (iter.hasNext())) {
            owner = (LockInfo) iter.next();
        }

        while (owner != null) {
            if (allOwnersAreReaders && !owner.isReadLock()) {
                allOwnersAreReaders = false;
            }
            if (allOwnersShareLocks &&
                anyOwnerLocker != null &&
                !anyOwnerLocker.sharesLocksWith(owner.getLocker())) {
                allOwnersShareLocks = false;
            }
            anyOwnerLocker = owner.getLocker();

            if ((iter != null) && (iter.hasNext())) {
                owner = (LockInfo) iter.next();
            } else {
                owner = null;
            }
        }

        /* 
         * Move the next set of waiters to the owners set. Iterate through
         * the firstWaiter field, then the waiterList.
         */

        boolean notifyOwners = false;
        LockInfo waiter = null;
        iter = null;
        boolean isFirstWaiter = false;

        if (waiterList != null) {
            iter = waiterList.iterator();
        }

        if (firstWaiter != null) {
            waiter = firstWaiter;
            isFirstWaiter = true;
        } else if ((iter != null) && (iter.hasNext())) {
            waiter = (LockInfo) iter.next();
        }
        
        while (waiter != null) {
            if (allOwnersAreReaders && !waiter.isReadLock()) {
                allOwnersAreReaders = false;
            }
            if (allOwnersShareLocks &&
                anyOwnerLocker != null &&
                !anyOwnerLocker.sharesLocksWith(waiter.getLocker())) {
                allOwnersShareLocks = false;
            }

            /*
             * Make the waiter an owner if
             * - there are no other owners, or
             * - all owners (including the waiter) can share locks (all are
             *   ThreadLocker instances for the same thread), or
             * - all owners (including the waiter) are readers, or
             * - there is only one owner for the same locker as the waiter
             *   (this occurs when a write was waiting on multiple reads)
             */
            Locker waiterLocker = waiter.getLocker();
            if (anyOwnerLocker == null ||
                allOwnersShareLocks ||
                allOwnersAreReaders ||
                (nOwners() == 1 && anyOwnerLocker == waiterLocker)) {
                /* Check for an existing owner for the same locker. */
                owner = getOwnerLockInfo(waiterLocker);
                if (owner == null) {
                    /* The waiter is a new owner. */
                    addOwner(waiter);
                } else {
                    /* Promote a read lock or reuse a write lock. */
                    LockType waiterType = waiter.getLockType();
                    if (waiterType == LockType.WRITE) {
                        owner.setLockType(waiterType);
                    }
                }

                if (isFirstWaiter) {
                    firstWaiter = null;
                } else {
                    iter.remove();
                }

                anyOwnerLocker = waiter.getLocker();
                notifyOwners = true;
            } else {
                /* Stop on first waiter that cannot be an owner. */
                break;
            }

            /* Move to the next waiter, it's in the list. */
            if ((iter != null) && (iter.hasNext())) {
                waiter = (LockInfo) iter.next();
                isFirstWaiter = false;
            } else {
                waiter = null;
            }
        }
        return (notifyOwners)? RELEASE_NOTIFY : RELEASE_NO_NOTIFY;
    }

    /*
     * Downgrade a write lock to a read lock.
     */
    void demote(Locker locker) {
        LockInfo owner = getOwnerLockInfo(locker);
        if ((owner != null) && (owner.isWriteLock())) {
            owner.setLockType(LockType.READ);
            return;
        }
    }

    /*
     * Transfer a lock from one transaction to another. Make sure that this
     * destination locker is only present as a single reader or writer.
     */
    LockType transfer(Locker currentLocker, Locker destLocker) 
        throws DatabaseException {

        /* 
         * Remove all the old owners held by the dest locker. Take
         * all the owners held by currentLocker and make them dest lockers.
         */
        LockType lockType = null;
        
        if (firstOwner != null) {
            if (firstOwner.getLocker() == destLocker) {
                firstOwner = null;
            } else if (firstOwner.getLocker() == currentLocker) {
                lockType = setNewLocker(firstOwner, destLocker);
            }
        }

        if (ownerSet != null) {
            Iterator iter = ownerSet.iterator();
            while (iter.hasNext()) {
                LockInfo owner = (LockInfo) iter.next();
                if (owner.getLocker() == destLocker) {
                    iter.remove();
                } else if (owner.getLocker() == currentLocker) {
                    lockType = setNewLocker(owner, destLocker);
                }
            }
        }

        /* Check the waiters, remove any that belonged to the dest locker */
        if ((firstWaiter != null) && (firstWaiter.getLocker() == destLocker)) {
            firstWaiter = null;
        } 
        if (waiterList != null) {
            Iterator iter = waiterList.iterator();
            while (iter.hasNext()) {
                LockInfo info = (LockInfo) iter.next();
                if (info.getLocker() == destLocker) {
                    iter.remove();
                }
            }
        }
        return lockType;
    }

    private LockType setNewLocker(LockInfo owner,
                              Locker destLocker) 
        throws DatabaseException {
        	
        owner.setLocker(destLocker);
        destLocker.addLock(nodeId, this, owner.getLockType(), 
                           LockGrantType.NEW);
        return owner.getLockType();
    }

    /*
     * Transfer a lock from one transaction to many others. Only really needed
     * for case where a write handle lock is being transferred to
     * multiple read handles.
     */
    LockType transferMultiple(Locker currentLocker, Locker [] destLockers) 
        throws DatabaseException {
        
        LockType lockType = null;
        LockInfo oldOwner = null;

        if (destLockers.length == 1) {
            return transfer(currentLocker, destLockers[0]);
        } else {
            /*
             * First remove any ownership of the dest txns 
             */
            if (firstOwner != null) {
                for (int i = 0; i < destLockers.length; i++) {
                    if (firstOwner.getLocker() == destLockers[i]) {
                        firstOwner = null;
                        break;
                    }
                }
            }
                 
            if (ownerSet != null) {
                Iterator ownersIter = ownerSet.iterator();
                while (ownersIter.hasNext()) {
                    LockInfo o = (LockInfo) ownersIter.next();
                    for (int i = 0; i < destLockers.length; i++) {
                        if (o.getLocker() == destLockers[i]) {
                            ownersIter.remove();
                            break;
                        }
                    }
                }
            }

            /* 
             * Create the clones 
             */
            if (firstOwner != null) {
                oldOwner = cloneLockInfo(firstOwner, currentLocker,
                                         destLockers);
            }

            if ((ownerSet != null) && (oldOwner == null))  {
                Iterator ownersIter = ownerSet.iterator();
                while (ownersIter.hasNext()) {
                    LockInfo o = (LockInfo) ownersIter.next();
                    oldOwner = cloneLockInfo(o, currentLocker,
                                             destLockers);
                    if (oldOwner != null) {
                        break;
                    }
                }
            }

            /* 
             * Check the waiters, remove any that belonged to the dest
             * locker
             */
            if (firstWaiter != null) {
                for (int i = 0; i < destLockers.length; i++) {
                    if (firstWaiter.getLocker() == destLockers[i]) {
                        firstWaiter = null;
                        break;
                    }
                }
            }
                        
            if (waiterList != null) {
                Iterator iter = waiterList.iterator();
                while (iter.hasNext()) {
                    LockInfo o = (LockInfo) iter.next();
                    for (int i = 0; i < destLockers.length; i++) {
                        if (o.getLocker() == destLockers[i]) {
                            iter.remove();
                            break;
                        }
                    }
                }
            }
        }
            
        boolean removed = flushOwner(oldOwner);
        assert removed;
        return lockType;
    }

    /**
     * If oldOwner is the current owner, clone it and transform it into a dest
     * locker.
     */
    private LockInfo cloneLockInfo(LockInfo oldOwner,
                                   Locker currentLocker,
                                   Locker [] destLockers) 
           throws DatabaseException {

        if (oldOwner.getLocker() == currentLocker) {
            try {
                LockType lockType = oldOwner.getLockType();
                for (int i = 0; i < destLockers.length; i++) {
                    LockInfo clonedLockInfo = (LockInfo) oldOwner.clone();
                    clonedLockInfo.setLocker(destLockers[i]);
                    destLockers[i].addLock(nodeId, this, lockType,
                                           LockGrantType.NEW);
                    addOwner(clonedLockInfo);
                }
                return oldOwner;
            } catch (CloneNotSupportedException e) {
                throw new DatabaseException(e);
            }
        } else {
            return null;
        }
    }

    /**
     * Return the locker that has a write ownership on this lock. If no
     * write owner exists, return null.
     */
    Locker getWriteOwnerLocker() {

        LockInfo foundOwner = null;
        if (firstOwner != null) {
            if ((ownerSet == null) || (ownerSet.size() == 0)) {
                /* One owner exists, it's the firstOwner. */
                foundOwner = firstOwner;
            } 
        } else {
            if ((ownerSet != null) && (ownerSet.size() == 1)) {
                /* One owner exists, and it's in the set, go get it. */
                Iterator iter = ownerSet.iterator();
                while (iter.hasNext()) {
                    foundOwner = (LockInfo) iter.next();
                    break;
                }
            }
        }

        if ((foundOwner != null) &&
            (foundOwner.getLockType() == LockType.WRITE)) { 
            return foundOwner.getLocker();
        } else {
            return null;
        }
    }

    /**
     * Debugging aid, validation before a lock request
     */
    private boolean validateRequest(Locker locker) {
        if (firstWaiter != null) {
            if (firstWaiter.getLocker() == locker) {
                assert false : "locker " + locker +
                                " is already on waiters list.";
            }
        }
         
        if (waiterList != null) {
            Iterator iter = waiterList.iterator();
            while (iter.hasNext()) {
                LockInfo o = (LockInfo) iter.next();
                if (o.getLocker() == locker) {
                    assert false : "locker " + locker +
                        " is already on waiters list.";
                }
            }
        }
        return true;
    }

    /**
     * Debug dumper
     */
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(" NodeId:").append(nodeId);
        sb.append(" Owners:");
        if (nOwners() == 0) {
            sb.append(" (none)");
        } else {
            if (firstOwner != null) {
                sb.append(firstOwner);
            }

            if (ownerSet != null) {
                Iterator iter = ownerSet.iterator();
                while (iter.hasNext()) {
                    LockInfo info = (LockInfo) iter.next();
                    sb.append(info);
                }
            }
        }

        sb.append(" Waiters:");
        if (nWaiters() == 0) {
            sb.append(" (none)");
        } else {
            sb.append(getWaitersListClone());
        }
        return sb.toString();
    }
    }
