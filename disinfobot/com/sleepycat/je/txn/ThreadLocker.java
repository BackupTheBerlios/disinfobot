/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: ThreadLocker.java,v 1.1 2004/11/22 18:27:57 kate Exp $
*/

package com.sleepycat.je.txn;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.tree.LN;

/**
 * Extends BasicLocker to share locks among all lockers for the same thread.
 * This class is used when a JE entry point is called with a null
 * transaction parameter.
 */
public class ThreadLocker extends BasicLocker {

    private Thread thread;

    /**
     * Creates a ThreadLocker.
     */
    public ThreadLocker(EnvironmentImpl env)
        throws DatabaseException {

        super(env);
        thread = Thread.currentThread();
    }

    /**
     * Check that this txn is not used in the wrong thread.
     */
    private void checkState()
        throws DatabaseException {

        if (thread != Thread.currentThread()) {
            throw new DatabaseException("A per-thread transaction was" +
                                        " created in " + thread +
                                        " but used in " +
                                        Thread.currentThread());
        }
    }

    /**
     * Override readLock in order to check thread.
     */
    public LockGrantType readLock(LN ln)
        throws DatabaseException {

        checkState();
        return super.readLock(ln);
    }

    /**
     * Override nonBlockingReadLock in order to check thread.
     */
    public LockGrantType nonBlockingReadLock(LN ln)
        throws DatabaseException {

        checkState();
        return super.nonBlockingReadLock(ln);
    }

    /**
     * Override writeLock in order to check thread.
     */
    public LockResult writeLock(LN ln, DatabaseImpl database)
        throws DatabaseException {

        checkState();
        return super.writeLock(ln, database);
    }

    /**
     * Override nonBlockingWriteLock in order to check thread.
     */
    public LockGrantType nonBlockingWriteLock(LN ln)
        throws DatabaseException {

        checkState();
        return super.nonBlockingWriteLock(ln);
    }

    /**
     * Creates a new instance of this txn for the same environment.
     */
    public Locker newInstance()
        throws DatabaseException {

        checkState();
        return new ThreadLocker(envImpl);
    }

    /**
     * Returns whether this txn can share locks with the given txn.
     * This is the true when both are txns are ThreadLocker instances for the
     * same thread.
     */
    public boolean sharesLocksWith(Locker txn) {
        try {
            ThreadLocker other = (ThreadLocker) txn;
            return thread == other.thread;
        } catch (ClassCastException e) {
            return false;
        }
    }
}
