/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2000-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: TransactionStats.java,v 1.1 2004/11/22 18:27:59 kate Exp $
*/

package com.sleepycat.je;

/**
 * Javadoc for this public class is generated
 * via the doc templates in the doc_src directory.
 */
public class TransactionStats {

    /**
     * The time the last completed checkpoint finished (as the number
     * of seconds since the Epoch, returned by the IEEE/ANSI Std
     * 1003.1 (POSIX) time interface).
     */
    private long lastCheckpointTime;

    /**
     * The last transaction ID allocated.
     */
    private long lastTxnId;

    /**
     * The number of transactions that are currently active.
     */
    private int nActive;

    /**
     * The number of transactions that have begun.
     */
    private int nBegins;

    /**
     * The number of transactions that have aborted.
     */
    private int nAborts;

    /**
     * The number of transactions that have committed.
     */
    private int nCommits;

    /**
     * The array of active transactions. Each element of the array is
     * an object of type DbTxnStat.Active.
     */
    private Active activeTxns[];

    protected TransactionStats() {
    }

    /**
     * The Active class represents an active transaction.
     *
     * FindBugs whines about this class not being static.
     */
    public class Active {
	/**
	 * The transaction ID of the transaction.
	 */
	public long txnId;

	/**
	 * The transaction ID of the parent transaction (or 0, if no parent).
	 */
	public long parentId;

	/**
	 * Internal use.
	 */
        public Active(long txnId, long parentId) {
            this.txnId = txnId;
            this.parentId = parentId;
        }

	public String toString() {
	    return "txnId = " + txnId;
	}
    }

    /**
     */
    public Active[] getActiveTxns() {
        return activeTxns;
    }

    /**
     */
    public long getLastCheckpointTime() {
        return lastCheckpointTime;
    }

    /**
     */
    public long getLastTxnId() {
        return lastTxnId;
    }

    /**
     */
    public int getNAborts() {
        return nAborts;
    }

    /**
     */
    public int getNActive() {
        return nActive;
    }

    /**
     */
    public int getNBegins() {
        return nBegins;
    }

    /**
     */
    public int getNCommits() {
        return nCommits;
    }

    /**
     * @param actives
     */
    public void setActiveTxns(Active[] actives) {
        activeTxns = actives;
    }

    /**
     * @param l
     */
    public void setLastCheckpointTime(long l) {
        lastCheckpointTime = l;
    }

    /**
     * @param val
     */
    public void setLastTxnId(long val) {
        lastTxnId = val;
    }

    /**
     * @param val
     */
    public void setNAborts(int val) {
        nAborts = val;
    }

    /**
     * @param val
     */
    public void setNActive(int val) {
        nActive = val;
    }

    /**
     * @param val
     */
    public void setNBegins(int val) {
        nBegins = val;
    }

    /**
     * @param val
     */
    public void setNCommits(int val) {
        nCommits = val;
    }

}
