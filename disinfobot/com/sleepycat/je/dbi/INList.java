/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: INList.java,v 1.1 2004/11/22 18:27:59 kate Exp $
*/

package com.sleepycat.je.dbi;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.latch.Latch;
import com.sleepycat.je.tree.IN;

/**
 * The INList is a list of in-memory INs for a given environment. 
 */
public class INList {
    private static final String DEBUG_NAME = INList.class.getName();
    private SortedSet ins = null; 
    private Set addedINs = null;
    private EnvironmentImpl envImpl;

    /* If both latches are acquired, major must always be acquired first. */
    private Latch majorLatch;
    private Latch minorLatch;

    private boolean updateMemoryUsage;

    INList(EnvironmentImpl envImpl) {
        this.envImpl = envImpl;
        ins = new TreeSet();
	addedINs = new HashSet();
        majorLatch = new Latch(DEBUG_NAME + " Major Latch", envImpl);
        minorLatch = new Latch(DEBUG_NAME + " Minor Latch", envImpl);
        updateMemoryUsage = true;
    }

    /**
     * Used only by tree verifier when validating INList. Must be called
     * with orig.majorLatch acquired.
     */
    public INList(INList orig, EnvironmentImpl envImpl)
	throws DatabaseException {

	ins = new TreeSet(orig.getINs());
	addedINs = new HashSet();	
        this.envImpl = envImpl;
        majorLatch = new Latch(DEBUG_NAME + " Major Latch", envImpl);
        minorLatch = new Latch(DEBUG_NAME + " Minor Latch", envImpl);
        updateMemoryUsage = false;
    }

    /* 
     * We ignore latching on this method because it's only called from
     * validate which ignores latching anyway.
     */
    public SortedSet getINs() {
	return ins;
    }

    /* 
     * Don't require latching, ok to be imprecise.
     */
    public int getSize() {
	return ins.size();
    }

    /**
     * An IN has just come into memory, add it to the list.
     */
    public void add(IN in)
	throws DatabaseException {

	/**
	 * We may end up calling add with the major latch already held.
	 * This can happen (e.g.) when the INCompressor takes the major
	 * latch and then calls search, which then fetches in a node
	 * that has been evicted which then calls INList.add.
	 */
	boolean enteredWithLatchHeld = majorLatch.isOwner();
        try {
            if (enteredWithLatchHeld || majorLatch.acquireNoWait()) {
                addAndSetMemory(ins, in);
            } else {
                minorLatch.acquire();
                addAndSetMemory(addedINs, in);
                minorLatch.release();

                /*
                 * Holder of the majorLatch may have released it.  If so, put
                 * addedINs contents on ins and clear addedINs.
                 */
                if (majorLatch.acquireNoWait()) {
                    minorLatch.acquire();
                    ins.addAll(addedINs);
                    addedINs.clear();
                }
            }
        } finally {
            releaseMinorLatchIfHeld();
            if (!enteredWithLatchHeld) {
                releaseMajorLatchIfHeld();
            }
        }
    }
    
    private void addAndSetMemory(Set set, IN in) {
        boolean addOk  = set.add(in);

        assert addOk : "failed adding in " + in.getNodeId();

        if (updateMemoryUsage) {
            MemoryBudget mb =  envImpl.getMemoryBudget();
            mb.updateCacheMemoryUsage(in.getInMemorySize());
            in.setInListResident(true);
        }
    }

    /**
     * An IN is getting swept or is displaced by recovery.  Caller is
     * responsible for acquiring the major latch before calling this
     * and releasing it when they're done.
     */
    public void removeLatchAlreadyHeld(IN in)
	throws DatabaseException {

	assert majorLatch.isOwner();
	
        boolean done = ins.remove(in);
        assert done;
        if (updateMemoryUsage) {
            envImpl.getMemoryBudget().
                updateCacheMemoryUsage(0-in.getInMemorySize());
            in.setInListResident(false);
        }
    }

    /**
     * An IN is getting swept or is displaced by recovery.
     */
    public void remove(IN in)
	throws DatabaseException {

        majorLatch.acquire();
        try {
            removeLatchAlreadyHeld(in);
        } finally {
            releaseMajorLatch();
        }
    }

    public SortedSet tailSet(IN in)
	throws DatabaseException {

	assert majorLatch.isOwner();
	return ins.tailSet(in);
    }

    public IN first()
	throws DatabaseException {

	assert majorLatch.isOwner();
	return (IN) ins.first();
    }

    /**
     * Return an iterator over the main 'ins' set.  Returned iterator
     * will not show the elements in addedINs.
     *
     * The major latch should be held before entering.  The caller is
     * responsible for releasing the major latch when they're finished
     * with the iterator.
     *
     * @return an iterator over the main 'ins' set.
     */
    public Iterator iterator() {
	assert majorLatch.isOwner();
	return ins.iterator();
    }

    /**
     * Use with caution: expects major latch to be held
     */
    public boolean contains(IN in)
	throws DatabaseException {

	minorLatch.acquire();
        boolean contains = ins.contains(in) || addedINs.contains(in);
	minorLatch.release();
	return contains;
    }

    /**
     * Clear the entire list at shutdown.
     */
    public void clear()
	throws DatabaseException {

        majorLatch.acquire();
	minorLatch.acquire();
        ins.clear();
	addedINs.clear();
	minorLatch.release();
        releaseMajorLatch();
    }

    public void dump() {
        System.out.println("size=" + getSize());
	Iterator iter = ins.iterator();
	while (iter.hasNext()) {
	    IN theIN = (IN) iter.next();
	    System.out.println("db=" + theIN.getDatabase().getId() +
                               " nid=: " + theIN.getNodeId() + "/" +
			       theIN.getLevel());
	}
    }

    public void latchMajor()
	throws DatabaseException {

	majorLatch.acquire();
    }

    public void releaseMajorLatchIfHeld()
	throws DatabaseException {

	if (majorLatch.isOwner()) {
	    releaseMajorLatch();
	}
    }

    public void releaseMajorLatch()
	throws DatabaseException {

	/*
	 * Before we release the major latch, take a look at addedINs
	 * and see if anything has been added to it while we held the
	 * major latch.  If so, added it to ins.
	 */
	latchMinor();
	if (addedINs.size() > 0) {
	    ins.addAll(addedINs);
	    addedINs.clear();
	}
	releaseMinorLatch();
	majorLatch.release();
    }

    private void latchMinor()
	throws DatabaseException {

	minorLatch.acquire();
    }

    private void releaseMinorLatch()
	throws DatabaseException {

	minorLatch.release();
    }

    private void releaseMinorLatchIfHeld()
	throws DatabaseException {

	if (minorLatch.isOwner()) {
	    releaseMinorLatch();
	}
    }


    /**
     * Lower the generation of any IN's belonging to this deleted database
     * so they evict quickly.
     */
    void clearDb(DatabaseImpl dbImpl)
	throws DatabaseException {

        DatabaseId id = dbImpl.getId();
        majorLatch.acquire();

	Iterator iter = ins.iterator();
	while (iter.hasNext()) {
	    IN theIN = (IN) iter.next();
	    if (theIN.getDatabase().getId().equals(id)) {
		theIN.setGeneration(0);
	    }
	}

        releaseMajorLatch();
    }
}
