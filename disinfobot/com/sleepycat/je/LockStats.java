/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: LockStats.java,v 1.1 2004/11/22 18:27:59 kate Exp $
*/

package com.sleepycat.je;

import com.sleepycat.je.latch.LatchStats;


/**
 * Lock statistics for a single environment.
 */
public class LockStats {

    /**
     * Total locks currently in lock table.
     */
    private int nTotalLocks;

    /**
     * Total read locks currently held.
     */
    private int nReadLocks;

    /**
     * Total write locks currently held.
     */
    private int nWriteLocks;

    /**
     * Total transactions waiting for locks.
     */
    private int nWaiters;

    /**
     * Total lock owners in lock table.
     */
    private int nOwners;

    /**
     * Number of times a lock request blocked.
     */
    private long nWaits;

    /**
     * LockTable latch stats.
     */
    private LatchStats lockTableLatchStats;

    /**
     */
    public int getNOwners() {
        return nOwners;
    }

    /**
     */
    public int getNReadLocks() {
        return nReadLocks;
    }

    /**
     */
    public int getNTotalLocks() {
        return nTotalLocks;
    }       

    /**
     */
    public int getNWaiters() {
        return nWaiters;
    }

    /**
     */
    public int getNWriteLocks() {
        return nWriteLocks;
    }

    /**
     */
    public long getNWaits() {
        return nWaits;
    }

    /**
     * @param val
     */
    public void setNOwners(int val) {
        nOwners = val;
    }

    /**
     * @param val
     */
    public void setNReadLocks(int val) {
        nReadLocks = val;
    }

    /**
     * @param val
     */
    public void setNTotalLocks(int val) {
        nTotalLocks = val;
    }

    /**
     * @param val
     */
    public void setNWaiters(int val) {
        nWaiters = val;
    }

    /**
     * @param val
     */
    public void setNWriteLocks(int val) {
        nWriteLocks = val;
    }

    /**
     */
    public void setNWaits(long waits) {
        this.nWaits = waits; 
    }

    /**
     */
    public void setLockTableLatchStats(LatchStats latchStats) {
        lockTableLatchStats = latchStats;
    }

    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("nTotalLocks=").append(nTotalLocks).append('\n');
        sb.append("nReadLocks=").append(nReadLocks).append('\n');
        sb.append("nWriteLocks=").append(nWriteLocks).append('\n');
        sb.append("nWaiters=").append(nWaiters).append('\n');
        sb.append("nOwners=").append(nOwners).append('\n');
        sb.append("nWaits=").append(nWaits).append('\n');
        sb.append("lockTableLatch:\n").append(lockTableLatchStats);
        return sb.toString();    
    }
}
