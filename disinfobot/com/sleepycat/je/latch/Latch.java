/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: Latch.java,v 1.1 2004/11/22 18:27:59 kate Exp $
*/

package com.sleepycat.je.latch;

import java.util.ArrayList;
import java.util.List;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.RunRecoveryException;
import com.sleepycat.je.dbi.EnvironmentImpl;

/**
 * Simple thread-based non-transactional exclusive non-nestable latch.
 * <p>
 * Latches provide simple exclusive transient locks on objects.
 * Latches are expected to be held for short, defined periods of time.
 * No deadlock detection is provided so it is the caller's
 * responsibility to sequence latch acquisition in an ordered fashion
 * to avoid deadlocks.
 * <p>
 * A latch can be acquire in wait or no-wait modes.  In the former,
 * the caller will wait for any conflicting holders to release the
 * latch.  In the latter, if the latch is not available, control
 * returns to the caller immediately.
 */
public class Latch {
    private static final String DEFAULT_LATCH_NAME = "Latch";

    /* Used for debugging */
    private static LatchTable latchTable = new LatchTable("Latch");

    private String name = null;
    private List waiters = null;
    private LatchStats stats = new LatchStats();
    /* The object that the latch protects. */
    private EnvironmentImpl env = null;
    private Thread owner = null;

    /**
     * Create a latch.
     */
    public Latch(String name, EnvironmentImpl env) {
	this.name = name;
	this.env = env;
    }

    /**
     * Create a latch with no name, more optimal for shortlived latches.
     */
    public Latch(EnvironmentImpl env) {
	this.env = env;
        this.name = DEFAULT_LATCH_NAME;
    }

    /**
     * Set the latch name, used for latches in objects instantiated from
     * the log.
     */
    synchronized public void setName(String name) {
        this.name = name;
    }

    /**
     * Acquire a latch for exclusive/write access.
     *
     * <p>Wait for the latch if some other thread is holding it.  If there are
     * threads waiting for access, they will be granted the latch on a FIFO
     * basis.  When the method returns, the latch is held for exclusive
     * access.</p>
     *
     * @throws LatchException if the latch is already held by the calling
     * thread.
     *
     * @throws RunRecoveryException if an InterruptedException exception
     * occurs.
     */
    public synchronized void acquire()
	throws DatabaseException {

        try {
            Thread thread = Thread.currentThread();
            if (thread == owner) {
                stats.nAcquiresSelfOwned++;
                throw new LatchException(getNameString() +
                                         " already held");
            }
            if (owner == null) {
                stats.nAcquiresNoWaiters++;
                owner = thread;
            } else {
                if (waiters == null) {
                    waiters = new ArrayList();
                }
                waiters.add(thread);
                stats.nAcquiresWithContention++;
                while (thread != owner) {
                    wait();
                }
            }
            assert noteLatch(); // intentional side effect;
	} catch (InterruptedException e) {
	    throw new RunRecoveryException(env, e);
	} finally {
	    if (EnvironmentImpl.getForcedYield()) {
		Thread.yield();
	    }
	}
    }

    /**
     * Acquire a latch for exclusive/write access, but do not block if it's not
     * available.
     *
     * @return true if the latch was acquired, false if it is not available.
     *
     * @throws LatchException if the latch is already held by the calling
     * thread.
     */
    public synchronized boolean acquireNoWait()
	throws LatchException {

        try {
            Thread thread = Thread.currentThread();
            if (thread == owner) {
                stats.nAcquiresSelfOwned++;
                throw new LatchException(getNameString() +
                                         " already held");
            }
            if (owner == null) {
                owner = thread;
                stats.nAcquireNoWaitSuccessful++;
                assert noteLatch(); // intentional side effect;
                return true;
            } else {
                stats.nAcquireNoWaitUnsuccessful++;
                return false;
            }
	} finally {
	    if (EnvironmentImpl.getForcedYield()) {
		Thread.yield();
	    }
	}
    }

    /**
     * Release the latch.  If there are other thread(s) waiting for the latch,
     * they are woken up and granted the latch.
     *
     * @throws LatchNotHeldException if the latch is not currently held.
     */
    public synchronized void release()
	throws LatchNotHeldException {

        try {
            Thread thread = Thread.currentThread();
            if (thread != owner) {
                throw new LatchNotHeldException(getNameString() +
                                                " not held");
            }
            if (waiters != null && waiters.size() > 0) {
                owner = (Thread) waiters.remove(0);
                notifyAll();
            } else {
                owner = null;
            }
            stats.nReleases++;
            assert unNoteLatch(); // intentional side effect.
	} finally {
	    if (EnvironmentImpl.getForcedYield()) {
		Thread.yield();
	    }
	}
    }

    /**
     * Return true if the current thread holds this latch.
     *
     * @return true if we hold this latch.  False otherwise.
     */
    public boolean isOwner() {

        return Thread.currentThread() == owner;
    }

    /**
     * Used only for unit tests.
     *
     * @return the thread that currently holds the latch for exclusive access.
     */
    public Thread owner() {

	return owner;
    }

    /**
     * Return the number of threads waiting.
     *
     * @return the number of threads waiting for the latch.
     */
    synchronized int nWaiters() {

        return (waiters != null) ? waiters.size() : 0;
    }

    /**
     * @return a LatchStats object with information about this latch.
     */
    public LatchStats getLatchStats() {
	LatchStats s = null;
	try {
	    s = (LatchStats) stats.clone();
	} catch (CloneNotSupportedException e) {
	}
	return s;
    }

    /**
     * Formats a latch owner and waiters.
     */
    public synchronized String toString() {

        return latchTable.toString(name, owner, waiters, 0);
    }

    /**
     * For concocting exception messages
     */
    private String getNameString() {

        return latchTable.getNameString(name);
    }

    /**
     * Only call under the assert system. This records latching by thread.
     */
    private boolean noteLatch()
	throws LatchException {

        return latchTable.noteLatch(this);
    }

    /**
     * Only call under the assert system. This records latching by thread.
     */
    private boolean unNoteLatch()
	throws LatchNotHeldException {

        return latchTable.unNoteLatch(this, name);
    }
    
    /**
     * Only call under the assert system. This records counts held latches.
     */
    static public int countLatchesHeld() {

        return latchTable.countLatchesHeld();
    }

    static public void dumpLatchesHeld() {

        System.out.println(latchesHeldToString());
    }

    static public String latchesHeldToString() {

        return latchTable.latchesHeldToString();
    }

    static public void clearNotes() {

        latchTable.clearNotes();
    }
}
