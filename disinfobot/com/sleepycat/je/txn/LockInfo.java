/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: LockInfo.java,v 1.1 2004/11/22 18:27:57 kate Exp $
*/

package com.sleepycat.je.txn;

/**
 * LockInfo is a class that embodies information about a lock
 * instance.  The holding thread and the locktype
 * are all contained in the object.
 */
public class LockInfo implements Cloneable {
    private Locker locker;
    private LockType lockType;

    /**
     * Construct a new LockInfo.
     */
    LockInfo(Locker locker,
	     LockType lockType) {
	this.locker = locker;
	this.lockType = lockType;
    }

    /**
     * Change this lockInfo over to the prescribed locker.
     */
    void setLocker(Locker locker) {
	this.locker = locker;
    }

    /**
     * @return The transaction associated with this Lock.
     */
    Locker getLocker() {
	return locker;
    }

    /**
     * @return The LockType associated with this Lock.
     */
    void setLockType(LockType lockType) {
	this.lockType = lockType;
    }

    /**
     * @return The LockType associated with this Lock.
     */
    LockType getLockType() {
	return lockType;
    }

    /**
     * @return true if the lock instance is a read
     */
    boolean isReadLock() {
        return lockType == LockType.READ;
    }

    /**
     * @return true if the lock instance is a write
     */
    boolean isWriteLock() {
        return lockType == LockType.WRITE;
    }

    public Object clone() 
        throws CloneNotSupportedException {

        return super.clone();
    }

    /** 
     * Debugging
     */
    public void dump() {
	System.out.println(this);
    }

    public String toString() {
	return "<LockInfo locker=\"" + locker.toString() + "\" " +
	    "type=\"" + lockType.toString() + "\"/>";
    }
}
