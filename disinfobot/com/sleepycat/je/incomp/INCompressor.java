/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: INCompressor.java,v 1.1 2004/11/22 18:27:59 kate Exp $
*/

package com.sleepycat.je.incomp;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbTree;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.EnvironmentStatsInternal;
import com.sleepycat.je.dbi.INList;
import com.sleepycat.je.latch.Latch;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.BINReference;
import com.sleepycat.je.tree.ChildReference;
import com.sleepycat.je.tree.DBIN;
import com.sleepycat.je.tree.DIN;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.tree.Key;
import com.sleepycat.je.tree.Node;
import com.sleepycat.je.tree.NodeNotEmptyException;
import com.sleepycat.je.tree.Tree;
import com.sleepycat.je.tree.Tree.SearchType;
import com.sleepycat.je.utilint.DaemonThread;
import com.sleepycat.je.utilint.PropUtil;
import com.sleepycat.je.utilint.Tracer;

/**
 * The IN Compressor thread.
 */
public class INCompressor extends DaemonThread {
    private static final String TRACE_COMPRESS = "INCompress:";

    private EnvironmentImpl env;
    private long lockTimeout;

    // stats
    private int splitBins = 0;
    private int dbClosedBins = 0;
    private int cursorsBins = 0;
    private int nonEmptyBins = 0;
    private int processedBins = 0;
    private Map binRefQueue;
    private Latch binRefQueueLatch;

    public INCompressor(EnvironmentImpl env, long waitTime, String name)
	throws DatabaseException {

	super(waitTime, name, env);
        this.env = env;
	lockTimeout = PropUtil.microsToMillis(env.getConfigManager().getLong
            (EnvironmentParams.COMPRESSOR_LOCK_TIMEOUT));
        binRefQueue = new HashMap();
        binRefQueueLatch = new Latch(name + " BINReference queue", env);
    }

    public String toString() {
	StringBuffer sb = new StringBuffer();
	sb.append("<INCompressor name=\"").append(name).append("\"/>");
	return sb.toString();
    }

    synchronized public void clearEnv() {
	env = null;
    }

    synchronized public void verifyCursors()
	throws DatabaseException {

	/*
	 * Environment may have been closed.  If so, then our job here is done.
	 */
	if (env.isClosed()) {
	    return;
	}

	/* 
	 * Use a snapshot to verify the cursors.  This way we don't have to
	 * hold a latch while verify takes locks.
	 */
	List queueSnapshot = null;
	try {
	    binRefQueueLatch.acquire();
	    queueSnapshot = new ArrayList(binRefQueue.values());
	} finally {
	    if (binRefQueueLatch.isOwner()) {
		binRefQueueLatch.release();
	    }
	}

	Iterator it = queueSnapshot.iterator();
	while (it.hasNext()) {
	    BINReference binRef = (BINReference) it.next();
            DatabaseImpl db =
		env.getDbMapTree().getDb(binRef.getDatabaseId(), lockTimeout);
	    BIN bin = searchForBIN(db, binRef);
	    if (bin != null) {
		bin.verifyCursors();
		bin.getLatch().release();
	    }
	}
    }

    /**
     * The default work queue is not used because we need a map not a set.
     */
    public void addToQueue(Object o)
	throws DatabaseException {

        throw new DatabaseException
            ("INCompressor.addToQueue should never be called.");
    }

    public int getBinRefQueueSize()
        throws DatabaseException {

        binRefQueueLatch.acquire();
        int size = binRefQueue.size();
        binRefQueueLatch.release();
        return size;
    }

    /**
     * Adds the BIN and deleted Key to the queue if the BIN is not already in
     * the queue, or adds the deleted key to an existing entry if one exists.
     */
    public void addBinKeyToQueue(BIN bin, Key deletedKey)
	throws DatabaseException {

        binRefQueueLatch.acquire();
        addBinKeyToQueueAlreadyLatched(bin, deletedKey);
        binRefQueueLatch.release();

        wakeup();
    }

    /**
     * Adds the BINReference to the queue if the BIN is not already in the
     * queue, or adds the deleted keys to an existing entry if one exists.
     */
    public void addBinRefToQueue(BINReference binRef)
	throws DatabaseException {

        binRefQueueLatch.acquire();
        addBinRefToQueueAlreadyLatched(binRef);
        binRefQueueLatch.release();

        wakeup();
    }

    /**
     * Adds an entire collection of BINReferences to the queue at once.  Use
     * this to avoid latching for each add.
     */
    public void addMultipleBinRefsToQueue(Collection binRefs)
	throws DatabaseException {

	binRefQueueLatch.acquire();
	Iterator it = binRefs.iterator();
	while (it.hasNext()) {
	    BINReference binRef = (BINReference) it.next();
            addBinRefToQueueAlreadyLatched(binRef);
        }
	binRefQueueLatch.release();

	wakeup();
    }

    /**
     * Adds the BINReference with the latch held.
     */
    private void addBinRefToQueueAlreadyLatched(BINReference binRef) {

        Long node = new Long(binRef.getNodeId());
        BINReference existingRef = (BINReference) binRefQueue.get(node);
        if (existingRef != null) {
            existingRef.addDeletedKeys(binRef);
        } else {
            binRefQueue.put(node, binRef);
        }
    }

    /**
     * Adds the BIN and deleted Key with the latch held.
     */
    private void addBinKeyToQueueAlreadyLatched(BIN bin, Key deletedKey) {

        Long node = new Long(bin.getNodeId());
        BINReference existingRef = (BINReference) binRefQueue.get(node);
        if (existingRef != null) {
            if (deletedKey != null) {
                existingRef.addDeletedKey(deletedKey);
            }
        } else {
            BINReference binRef = bin.createReference();
            if (deletedKey != null) {
                binRef.addDeletedKey(deletedKey);
            }
            binRefQueue.put(node, binRef);
        }
    }

    /**
     * Return stats
     */
    public void loadStats(StatsConfig config, EnvironmentStatsInternal stat) 
        throws DatabaseException {

        stat.setSplitBins(splitBins);
        stat.setDbClosedBins(dbClosedBins);
        stat.setCursorsBins(cursorsBins);
        stat.setNonEmptyBins(nonEmptyBins);
        stat.setProcessedBins(processedBins);
        stat.setInCompQueueSize(getBinRefQueueSize());

        if (config.getClear()) {
            splitBins = 0;
            dbClosedBins = 0;
            cursorsBins = 0;
            nonEmptyBins = 0;
            processedBins = 0;
        }
    }

    /**
     * Return the number of retries when a deadlock exception occurs.
     */
    protected int nDeadlockRetries()
        throws DatabaseException {

        return env.getConfigManager().getInt
            (EnvironmentParams.COMPRESSOR_RETRY);
    }

    public synchronized void onWakeup()
	throws DatabaseException {

        if (env.isClosed()) {
            return;
        } 
        
        doCompress();
    }

    /**
     * The real work to doing a compress. This may be called by the compressor
     * thread or programatically.
     */
    public synchronized void doCompress() 
        throws DatabaseException {

	if (!isRunnable()) {
	    return;
	}

	/* Give the onWakeup() method a snapshot of the current work queue. */
	Map queueSnapshot = null;
	try {
	    binRefQueueLatch.acquire();
	    queueSnapshot = binRefQueue;
	    binRefQueue = new HashMap();
	} finally {
	    if (binRefQueueLatch.isOwner()) {
		binRefQueueLatch.release();
	    }
	}

	int splitBinsThisRun = 0;
	int dbClosedBinsThisRun = 0;
	int cursorsBinsThisRun = 0;
	int nonEmptyBinsThisRun = 0;
	int processedBinsThisRun = 0;
        List cursorsBinList = new ArrayList();
	
        Tracer.trace(Level.FINE, env,
                     "InCompress.doCompress called, queue size: " +
                     queueSnapshot.size());

	assert Latch.countLatchesHeld() == 0;

	INList inList = env.getInMemoryINs();

	try {
	    Iterator it = queueSnapshot.values().iterator();
	    while (it.hasNext()) {

		BINReference binRef = (BINReference) it.next();

		/*
		 * Environment may have been closed.  If so, then our
		 * job here is done.
		 */
		if (env.isClosed()) {
		    return;
		}

		DbTree dbTree = env.getDbMapTree();
                DatabaseImpl db =
		    dbTree.getDb(binRef.getDatabaseId(), lockTimeout);

                if (db == null) {
                    /* The db was deleted. Ignore this BIN Ref. */
                    dbClosedBinsThisRun++;
                    continue;
                }
                
		try {
		    inList.latchMajor();

		    Tracer.trace(Level.FINEST, env, "Compressing " + binRef);

                    boolean empty = false;
                    BIN bin = null;
                    Key idKey = null;
                    try {
                        bin = searchForBIN(db, binRef);
                        if ((bin == null) ||
                            bin.getNodeId() != binRef.getNodeId()) {
                            /* The BIN may have been split. */
                            splitBinsThisRun++;
                            continue;
                        }
                        
                        idKey = bin.getIdentifierKey();
                        int nCursors = bin.nCursors();
                        if (nCursors > 0) {

                            /* 
                             * There are cursors pointing to the BIN, so try
                             * again later.
                             */
                            addBinRefToQueue(binRef);
                            cursorsBinsThisRun++;
                            cursorsBinList.add(new Long(bin.getNodeId()));
                            continue;
                        }

                        empty = bin.compress(binRef);
                    } finally {
                        if (bin != null) {
                            bin.releaseLatch();
                        }
                    }

		    if (empty) {
			try {
                            Tree tree = db.getTree();

			    if (bin.containsDuplicates()) {
				DBIN dbin = (DBIN) bin;
				if (!tree.deleteDup(idKey,
						    dbin.getDupKey())) {
				    addBinRefToQueue(binRef);
				    cursorsBinsThisRun++;
				} else {
				    processedBinsThisRun++;
				}
			    } else {
				boolean deletedRoot = tree.delete(idKey);

				/*
				 * modifyDb will grab locks and we can't have
				 * the INList Major Latch held while it tries
				 * to acquire locks.  deletedRoot means that we
				 * were trying to delete the root.  Finish the
				 * job now.
				 */
				assert Latch.countLatchesHeld() == 1;
				if (deletedRoot) {
				    inList.releaseMajorLatchIfHeld();
				    dbTree.modifyDbRoot(db);
				    Tracer.traceRootDeletion(Level.FINE, db);
				    inList.latchMajor();
				}
				processedBinsThisRun++;
			    }
			} catch (NodeNotEmptyException NNNE) {
			    /* 
			     * Something happened to the node
			     * while we had it unlatched -- it might
			     * be an insertion or a cursor walked
			     * onto it. In any case, we can't
			     * process it right now.
			     */
			    nonEmptyBinsThisRun++;
			}

			/* INList major latch is held. */
			assert Latch.countLatchesHeld() == 1;
		    } else {
			nonEmptyBinsThisRun++;
		    }
		} finally {
		    inList.releaseMajorLatchIfHeld();
		}
	    }

            Tracer.trace(Level.FINE, env,
			 "splitBins = " + splitBinsThisRun +
			 " dbClosedBins = " + dbClosedBinsThisRun +
			 " cursorsBins = " + cursorsBinsThisRun +
			 " nonEmptyBins = " + nonEmptyBinsThisRun +
			 " processedBins = " + processedBinsThisRun);
            
	} catch (DatabaseException DBE) {
	    System.err.println("INCompressor caught: " + DBE);
	    DBE.printStackTrace();
	} finally {
	    assert Latch.countLatchesHeld() == 0;
	    accumulateStats(splitBinsThisRun, dbClosedBinsThisRun,
			    cursorsBinsThisRun, nonEmptyBinsThisRun,
			    processedBinsThisRun,
                            cursorsBinList);
	}
    }

    private boolean isRunnable()
	throws DatabaseException {

	return true;
    }

    /**
     * Search the tree for the BIN or DBIN that corresponds to this
     * BINReference.
     * 
     * @param binRef the BINReference that indicates the bin we want.
     * @return the BIN or DBIN that corresponds to this BINReference. The
     * node is latched upon return. Returns null if the BIN can't be found.
     */
    public BIN searchForBIN(DatabaseImpl db, BINReference binRef)
        throws DatabaseException {

        /* Search for this IN */
        Tree tree = db.getTree();
        IN in = tree.search(binRef.getKey(), SearchType.NORMAL, -1);

        /* Couldn't find a bin, return null */
        if (in == null) {
            return null;
        }

        /* This is not a duplicate, we're done. */
        if (binRef.getData() == null) {
            return (BIN) in;
        }

        /* We need to descend down into a duplicate tree. */
        DIN duplicateRoot = null;
        DBIN duplicateBin = null;
        BIN bin = (BIN) in;
        try {
            int index = bin.findEntry(binRef.getKey(), false, true);
            if (index >= 0) {
		ChildReference ref = bin.getEntry(index);
		if (ref.isKnownDeleted()) {
		    bin.releaseLatch();
		    return null;
		}
                Node node = ref.fetchTarget(db, bin);
                if (node.containsDuplicates()) {
                    /* It's a duplicate tree. */
                    duplicateRoot = (DIN) node;
                    duplicateRoot.latch();
                    bin.releaseLatch();
                    duplicateBin = (DBIN)
                        tree.searchSubTree(duplicateRoot, binRef.getData(),
					   SearchType.NORMAL, -1);

                    return duplicateBin;
                } else {
                    /* We haven't migrated to a duplicate tree yet.
                     * XXX, isn't this taken care of above? */
                    return bin;
                }
            } else {
                bin.releaseLatch();
                return null;
            }
        } catch (DatabaseException DBE) {
            if (bin != null &&
                bin.getLatch().isOwner()) {
                bin.releaseLatch();
            }
            if (duplicateRoot != null &&
                duplicateRoot.getLatch().isOwner()) {
                duplicateRoot.releaseLatch();
            }

	    /* 
	     * FindBugs whines about Redundent comparison to null below, but
	     * for stylistic purposes we'll leave it in.
	     */
            if (duplicateBin != null &&
                duplicateBin.getLatch().isOwner()) {
                duplicateBin.releaseLatch();
            }
            throw DBE;
        }
    }

    private void accumulateStats(int splitBinsThisRun,
				 int dbClosedBinsThisRun,
				 int cursorsBinsThisRun,
				 int nonEmptyBinsThisRun,
				 int processedBinsThisRun,
                                 List cursorsBinList) {
	splitBins += splitBinsThisRun;
	dbClosedBins += dbClosedBinsThisRun;
	cursorsBins += cursorsBinsThisRun;
	nonEmptyBins += nonEmptyBinsThisRun;
	processedBins += processedBinsThisRun;
	trace(Level.FINE,
              splitBinsThisRun,
              dbClosedBinsThisRun,
              cursorsBinsThisRun,
              nonEmptyBinsThisRun,
              processedBinsThisRun,
              cursorsBinList);
    }

    /**
     * Send trace messages to the java.util.logger. Don't rely on the logger
     * alone to conditionalize whether we send this message, we don't even want
     * to construct the message if the level is not enabled.
     */
    private void trace(Level level,
                       int splitBinsThisRun,
                       int dbClosedBinsThisRun,
                       int cursorsBinsThisRun,
                       int nonEmptyBinsThisRun,
                       int processedBinsThisRun,
                       List cursorsBinList) {
        Logger logger = env.getLogger();
        if (logger.isLoggable(level)) {
	    StringBuffer sb = new StringBuffer();
            sb.append(TRACE_COMPRESS);
            sb.append(" splitBins=").append(splitBinsThisRun);
            sb.append(" dbClosedBins=").append(dbClosedBinsThisRun);
            sb.append(" cursorsBins=").append(cursorsBinsThisRun);
            sb.append(" nonEmptyBins=").append(nonEmptyBinsThisRun);
            sb.append(" processedBins=").append(processedBinsThisRun);
            logger.log(level, sb.toString());
        }
        if (logger.isLoggable(Level.FINEST)) {
            StringBuffer sb = new StringBuffer();
            sb.append("cursorBinList:");
            for (int i = 0; i < cursorsBinList.size(); i++) {
                sb.append((Long)cursorsBinList.get(i)).append(" ");
            }
            logger.log(Level.FINEST, sb.toString());
        }
    }
}
