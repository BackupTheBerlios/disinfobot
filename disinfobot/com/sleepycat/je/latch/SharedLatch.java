/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: SharedLatch.java,v 1.1 2004/11/22 18:27:59 kate Exp $
*/

package com.sleepycat.je.latch;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.RunRecoveryException;
import com.sleepycat.je.dbi.EnvironmentImpl;

/**
 * Simple thread-based non-transactional reader-writer/shared-exclusive latch.
 * 
 * Latches provide simple exclusive or shared transient locks on objects.
 * Latches are expected to be held for short, defined periods of time.  No
 * deadlock detection is provided so it is the caller's responsibility to
 * sequence latch acquisition in an ordered fashion to avoid deadlocks.
 * 
 * Nested latches for a single thread are supported, but upgrading a shared
 * latch to an exclusive latch is not.  This implementation is based on the
 * section Reader-Writer Locks in the book Java Threads by Scott Oaks, 2nd
 * Edition, Chapter 8.
 */
public class SharedLatch {

    /* Used for debugging */
    private static LatchTable latchTable = new LatchTable("SharedLatch");

    private String name = null;
    private List waiters = new ArrayList();
    private LatchStats stats = new LatchStats();
    /* The object that the latch protects. */
    private Object subject = null;
    private EnvironmentImpl env = null;

    /**
     * Create a latch with a backpointer to its subject (the object that is
     * protected by the latch along with a name (both for debugging purposes).
     */
    public SharedLatch(Object subject, String name, EnvironmentImpl env) {
	this.subject = subject;
	this.name = name;
	this.env = env;
    }

    /**
     * Set the latch name, used for latches in objects instantiated from the
     * log.
     */
    synchronized public void setName(String name) {
        this.name = name;
    }

    /* Return the subject of this latch. */
    synchronized public Object getSubject() {
	return subject;
    }

    /**
     * Acquire a latch for exclusive/write access.  Nesting is allowed, that
     * is, the latch may be acquired more than once by the same thread for
     * exclusive access.  However, if the thread already holds the latch for
     * shared access, it cannot be upgraded and LatchException will be thrown.
     *
     * Wait for the latch if some other thread is holding it.  If there are
     * threads waiting for access, they will be granted the latch on a FIFO
     * basis.  When the method returns, the latch is held for exclusive access.
     *
     * @throws LatchException if the latch is already held by the current
     * thread for shared access.
     *
     * @throws RunRecoveryException if an InterruptedException exception
     * occurs.
     */
    public synchronized void acquireExclusive()
	throws DatabaseException {

        try {
            Thread thread = Thread.currentThread();
            int index = indexOf(thread);
            Owner owner;
            if (index < 0) {
                owner = new Owner(thread, Owner.EXCLUSIVE);
                waiters.add(owner);
            } else {
                owner = (Owner) waiters.get(index);
                if (owner.type == Owner.SHARED) {
                    stats.nAcquiresUpgrade++;
                    throw new LatchException(getNameString() +
                                             " upgrade not allowed");
                }
                assert owner.type == Owner.EXCLUSIVE;
            }
            if (waiters.size() == 1) {
                stats.nAcquiresNoWaiters++;
            } else {
                stats.nAcquiresWithContention++;
                while (waiters.get(0) != owner) {
                    wait();
                }
            }
            owner.nAcquires += 1;
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
     * Acquire a latch for shared/read access.  Nesting is allowed, that is,
     * the latch may be acquired more than once by the same thread.
     *
     * @throws RunRecoveryException if an InterruptedException exception
     * occurs.
     */
    public synchronized void acquireShared()
        throws DatabaseException {

        try {
            Thread thread = Thread.currentThread();
            int index = indexOf(thread);
            Owner owner;
            if (index < 0) {
                owner = new Owner(thread, Owner.SHARED);
                waiters.add(owner);
            } else {
                owner = (Owner) waiters.get(index);
            }
            while (indexOf(thread) > firstWriter()) {
                wait();
            }
            owner.nAcquires += 1;
            stats.nAcquireSharedSuccessful++;
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
     * Release an exclusive or shared latch.  If there are other thread(s)
     * waiting for the latch, they are woken up and granted the latch.
     *
     * @throws LatchNotHeldException if the latch is not currently held.
     */
    public synchronized void release()
	throws LatchNotHeldException {

        try {
            Thread thread = Thread.currentThread();
            int index = indexOf(thread);
            if (index < 0 || index > firstWriter()) {
                throw new LatchNotHeldException(getNameString() +
                                                " not held");
            }
            Owner owner = (Owner) waiters.get(index);
            owner.nAcquires -= 1;
            if (owner.nAcquires == 0) {
                waiters.remove(index);
                notifyAll();
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
     * Returns the index of the first Owner for the given thread, or -1 if
     * none.
     */
    private int indexOf(Thread thread) {

        Iterator i = waiters.iterator();
        for (int index = 0; i.hasNext(); index += 1) {
            Owner owner = (Owner) i.next();
            if (owner.thread == thread) {
                return index;
            }
        }
        return -1;
    }

    /**
     * Returns the index of the first Owner waiting for a write lock, or
     * Integer.MAX_VALUE if none.
     */
    private int firstWriter() {

        Iterator i = waiters.iterator();
        for (int index = 0; i.hasNext(); index += 1) {
            Owner owner = (Owner) i.next();
            if (owner.type == Owner.EXCLUSIVE) {
                return index;
            }
        }
        return Integer.MAX_VALUE;
    }

    /**
     * Holds the state of a single owner thread.
     */
    private static class Owner {

        static final int SHARED = 0;
        static final int EXCLUSIVE = 1;

        Thread thread;
        int type;
        int nAcquires;

        Owner(Thread thread, int type) {
            this.thread = thread;
            this.type = type;
        }
    }

    /**
     * Return true if the current thread holds this latch.
     *
     * @return true if we hold this latch.  False otherwise.
     */
    public boolean isOwner() {

        return Thread.currentThread() == owner();
    }

    /**
     * Used only for unit tests.
     *
     * @return the thread that currently holds the latch for exclusive access.
     */
    public synchronized Thread owner() {

	return (waiters.size() > 0) ? ((Owner) waiters.get(0)).thread : null;
    }

    /**
     * Return the number of threads waiting.
     *
     * @return the number of threads waiting for the latch.
     */
    synchronized int nWaiters() {

        int n = waiters.size();
        return (n > 0) ? (n - 1) : 0;
    }

    /**
     * Used for debugging latches.
     *
     * @return a LatchStats object with information about this latch.
     */
    LatchStats getLatchStats() {

	return stats;
    }

    /**
     * Formats a latch owner and waiters.
     */
    public synchronized String toString() {

        return latchTable.toString(name, owner(), waiters, 1);
    }

    /*
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
