/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: TxnManager.java,v 1.1 2004/11/22 18:27:57 kate Exp $
*/

package com.sleepycat.je.txn;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.LockStats;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.TransactionStats;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.latch.Latch;
import com.sleepycat.je.utilint.DbLsn;

/**
 * Class to manage transactions.  Basically a Set of all transactions
 * with add and remove methods and a latch around the set.
 */
public class TxnManager {
    /* 
     * All NullTxns share the same id so as not to eat from the id 
     * number space.
     */
    static final long NULL_TXN_ID = -1; 
    private static final String DEBUG_NAME = TxnManager.class.getName();
    
    private LockManager lockManager;
    private EnvironmentImpl env;
    private Latch allTxnLatch;
    private Set allTxns;
    private long lastUsedTxnId;
    
    /* Locker Stats */
    private int numCommits;
    private int numAborts;

    public TxnManager(EnvironmentImpl env) 
    	throws DatabaseException {
        if (env.getConfigManager().
            getBoolean(EnvironmentParams.ENV_FAIR_LATCHES)) {
            lockManager = new LatchedLockManager(env);
        } else {
            lockManager = new SyncedLockManager(env);
        }

        this.env = env;
        allTxns = new HashSet();
        allTxnLatch = new Latch(DEBUG_NAME, env);

        numCommits = 0;
        numAborts = 0;
        lastUsedTxnId = 0;
    }

    /**
     * Set the txn id sequence.
     */
    synchronized public void setLastTxnId(long lastId) {
        this.lastUsedTxnId = lastId;
    }

    /**
     * Get the last used id, for checkpoint info.
     */
    public synchronized long getLastTxnId() {
        return lastUsedTxnId;
    }

    /**
     * Get the next transaction id to use.
     */
    synchronized long incTxnId() {
        return ++lastUsedTxnId;
    }
    
    /**
     * Create a new transaction.
     * @param parent for nested transactions, not yet supported
     * @param txnConfig specifies txn attributes
     * @return the new txn
     */
    public Txn txnBegin(Transaction parent, TransactionConfig txnConfig) 
        throws DatabaseException {

        if (parent != null) {
            throw new DatabaseException
		("Nested transactions are not supported yet.");
        }
        
        return new Txn(env, txnConfig);
    }

    /**
     * Give transactions and environment access to lock manager.
     */
    public LockManager getLockManager() {
        return lockManager;
    }

    /**
     * Called when txn is created.
     */
    void registerTxn(Txn txn)
        throws DatabaseException {

        allTxnLatch.acquire();
        allTxns.add(txn);
        allTxnLatch.release();
    }

    /**
     * Called when txn ends.
     */
    void unRegisterTxn(Locker txn, boolean isCommit) 
        throws DatabaseException {

        allTxnLatch.acquire();
        allTxns.remove(txn);
        if (isCommit) {
            numCommits++;
        } else {
            numAborts++;
        }
        allTxnLatch.release();
    }

    /**
     * Get the earliest lsn of all the active transactions, for checkpoint.
     * XXX, too expensive?
     */
    public DbLsn getFirstActiveLsn() 
        throws DatabaseException {

        allTxnLatch.acquire();
        Iterator iter = allTxns.iterator();
        DbLsn firstActive = null;
        while(iter.hasNext()) {
            DbLsn txnFirstActive = ((Txn) iter.next()).getFirstActiveLsn();
            if (firstActive == null) {
                firstActive = txnFirstActive;
            } else if (txnFirstActive != null) {
                if (txnFirstActive.compareTo(firstActive) < 0) {
                    firstActive = txnFirstActive;
                }
            }
        }
        allTxnLatch.release();
        return firstActive;
    }

    /*
     * Statistics
     */

    /**
     * Collect transaction related stats
     */
    public TransactionStats txnStat(StatsConfig config)
        throws DatabaseException {

        TransactionStats stats = new TransactionStatsInternal();
        allTxnLatch.acquire();
        stats.setNCommits(numCommits);
        stats.setNAborts(numAborts);
        stats.setNActive(allTxns.size());
        TransactionStats.Active [] activeSet = new TransactionStats.Active[stats.getNActive()];
        stats.setActiveTxns(activeSet);
        Iterator iter = allTxns.iterator();
        int i = 0;
        while (iter.hasNext()) {
            Locker txn = (Locker) iter.next();
            activeSet[i] = stats.new Active(txn.getId(), 0);
            i++;
        }
        if (config.getClear()) {
            numCommits = 0;
            numAborts = 0;
        }
        allTxnLatch.release();
        return stats;
    }

    /**
     * Collect lock related stats
     */
    public LockStats lockStat(StatsConfig config) 
        throws DatabaseException {

        return lockManager.lockStat(config);
    }
}
