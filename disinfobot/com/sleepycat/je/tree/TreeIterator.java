/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: TreeIterator.java,v 1.1 2004/11/22 18:27:57 kate Exp $
*/

package com.sleepycat.je.tree;

import java.util.Iterator;
import java.util.NoSuchElementException;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.latch.LatchNotHeldException;

/**
 * TreeIterator implements an Iterator over Tree's.  Not protected
 * against insertions like cursors.
 */
public final class TreeIterator implements Iterator {
    private Tree tree;
    private BIN nextBin;
    private int index;

    public TreeIterator(Tree tree)
	throws DatabaseException {

	nextBin = (BIN) tree.getFirstNode();
	if (nextBin != null) {
	    nextBin.releaseLatch();
	}
	index = -1;
	this.tree = tree;
    }

    public boolean hasNext() {
	boolean ret = false;
        try {
            if (nextBin != null) {
                nextBin.latch();
            }
            advance();
            ret = (nextBin != null) && (index < nextBin.getNEntries());
        } catch (DatabaseException e) {
            // eat exception
        } finally {
            try {
                if (nextBin != null) {
                    nextBin.releaseLatch();
                }
            } catch (LatchNotHeldException e) {
                // eat exception
            }
        }
        return ret;
    }

    public Object next() {

        Object ret = null;
        try {
            if (nextBin == null) {
                throw new NoSuchElementException();
            }
            nextBin.latch();
            ret = nextBin.getEntry(index).getKey();
        } catch (DatabaseException e) {
	    // eat exception, return null;
        } finally {
            try {
                if (nextBin != null) {
                    nextBin.releaseLatch();
                }
            } catch (LatchNotHeldException e) {
                // eat exception
            }
        }
	return ret;
    }

    public void remove() {
	throw new UnsupportedOperationException();
    }

    private void advance()
	throws DatabaseException {

	while (nextBin != null) {
	    if (++index < nextBin.getNEntries()) {
		return;
	    }
	    nextBin.releaseLatch();
	    nextBin = tree.getNextBin(nextBin, null);
	    index = -1;
	}
    }
}
