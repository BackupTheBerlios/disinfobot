/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: Evictor.java,v 1.1 2004/11/22 18:27:59 kate Exp $
*/

package com.sleepycat.je.evictor;

import java.text.NumberFormat;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
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
import com.sleepycat.je.dbi.MemoryBudget;
import com.sleepycat.je.latch.Latch;
import com.sleepycat.je.log.LogManager;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.tree.Node;
import com.sleepycat.je.tree.SearchResult;
import com.sleepycat.je.tree.Tree;
import com.sleepycat.je.utilint.DaemonThread;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.LevelOrderedINMap;
import com.sleepycat.je.utilint.Tracer;

/**
 * The Evictor looks through the INList for IN's and BIN's that are worthy of
 * eviction.  Once the nodes are selected, it removes all references to them so
 * that they can be GC'd by the JVM.
 */
public class Evictor extends DaemonThread {
    private static final boolean DEBUG = false;

    private EnvironmentImpl envImpl;
    private Level detailedTraceLevel;  // level value for detailed trace msgs
    private volatile boolean active;   // true if eviction is happening.

    /**
     * A running pointer in the INList so we know where to start the next scan.
     */
    private IN nextNode;

    /**
     * The percentage of the INList to scan on each wakeup.
     */
    private int nodeScanPercentage;

    /**
     * The percentage of the scanned batch that should be evicted.  If memory
     * usage is high, raise this percentage for this run.
     */
    private int evictionBatchPercentage;

    /**
     * The number of bytes we need to evict in order to get under budget.
     */
    private long requiredEvictBytes;

    /**
     * An implementation of Comparator that takes two IN's and compares their
     * Generation counts.
     */
    static private class INGenerationComparator implements Comparator {
        public int compare(Object o1, Object o2) {
            if (!(o1 instanceof IN &&
                  o2 instanceof IN)) {
                throw new IllegalArgumentException
                    ("INGenerationComparator.compare received non-IN arg.");
            }
            long gen1 = ((IN) o1).getGeneration();
            long gen2 = ((IN) o2).getGeneration();
	    if (gen1 == gen2) {
		if (o1.equals(o2)) {
		    return 0;
		} else {
		    gen1 = ((IN) o1).getNodeId();
		    gen2 = ((IN) o2).getNodeId();
		}
	    }
            if (gen1 < gen2) {
                return -1;
            } else if (gen1 > gen2) {
                return 1;
            } else {
		return 0;
            } 
        }
    }

    private INGenerationComparator inGenerationComparator =
        new INGenerationComparator();

    private NumberFormat formatter; // for trace messages.

    /*
     * Stats
     */

    /* Number of passes made to the evictor. */
    private int nEvictPasses = 0;

    /* Number of nodes selected to evict. */
    private long nNodesSelected = 0;
    private long nNodesSelectedThisRun;

    /* Number of nodes scanned in order to select the eviction set */
    private int nNodesScanned = 0;
    private int nNodesScannedThisRun;

    /* 
     * Number of nodes evicted on this run. This could be understated, as a
     * whole subtree may have gone out with a single node.
     */
    private long nNodesEvicted = 0;
    private long nNodesEvictedThisRun;

    /* Number of BINs stripped. */
    private long nBINsStripped = 0;
    private long nBINsStrippedThisRun;

    private EvictProfile evictProfile;  // debugging

    public Evictor(EnvironmentImpl envImpl,
                   String name,
                   int nodeScanPercentage,
                   int evictionBatchPercentage)
        throws DatabaseException {

        super(0, name, envImpl);
        this.envImpl = envImpl;
        nextNode = null;
        this.nodeScanPercentage = nodeScanPercentage;
        this.evictionBatchPercentage = evictionBatchPercentage;

        detailedTraceLevel =
            Tracer.parseLevel(envImpl,
                              EnvironmentParams.JE_LOGGING_LEVEL_EVICTOR);
        evictProfile = new EvictProfile();
        formatter = NumberFormat.getNumberInstance();

        active = false;
    }

    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("<Evictor name=\"").append(name).append("\"/>");
        return sb.toString();
    }

    /**
     * Evictor doesn't have a work queue so just throw an exception if it's
     * ever called.
     */
    public void addToQueue(Object o)
        throws DatabaseException {

        throw new DatabaseException
            ("Evictor.addToQueue should never be called.");
    }

    /**
     * Load stats.
     */
    public void loadStats(StatsConfig config, EnvironmentStatsInternal stat) 
        throws DatabaseException {

        stat.setNEvictPasses(nEvictPasses);
        stat.setNNodesSelected(nNodesSelected);
        stat.setNNodesScanned(nNodesScanned);
        stat.setNNodesExplicitlyEvicted(nNodesEvicted);
        stat.setNBINsStripped(nBINsStripped);
        stat.setRequiredEvictBytes(requiredEvictBytes);

        if (config.getClear()) {
            nEvictPasses = 0;
            nNodesSelected = 0;
            nNodesScanned = 0;
            nNodesEvicted = 0;
            nBINsStripped = 0;
        }
    }

    synchronized public void clearEnv() {
        envImpl = null;
    }

    /**
     * Return the number of retries when a deadlock exception occurs.
     */
    protected int nDeadlockRetries()
        throws DatabaseException {

        return envImpl.getConfigManager().getInt
            (EnvironmentParams.EVICTOR_RETRY);
    }
    /**
     * Wakeup the evictor only if it's not already active.
     */
    public void alert() {
        if (!active) {
            wakeup();
        }
    }

    /**
     * Called whenever the daemon thread wakes up from a sleep. 
     */
    public synchronized void onWakeup()
        throws DatabaseException {

        if (envImpl.isClosed()) {
            return;
        }

        doEvict(false);
    }

    /**
     * May be called by the evictor thread on wakeup or programatically.
     * @param force if force is true, don't check memory budget, just run
     * regardless.
     */
    public synchronized void doEvict(boolean force) 
        throws DatabaseException {

        /* Eviction is running, don't notify the daemon. */
        active = true;

        /* Repeat as necessary to keep up with allocations. */
        try {
            boolean progress = true;
            while (progress && !isShutdownRequested() && isRunnable(force)) {
                if (evictBatch() == 0) {
                    progress = false;
                }
                force = false;
            }
        } finally {
            active = false;
        }
    }

    /**
     * Each iteration will latch and unlatch the major INList, and will attempt
     * to evict requiredEvictBytes, but will give up after two passes over the
     * major INList.  This is important, since the major INList needs to be
     * unlatched to allow new INs to be added, and possibily evicted.
     *
     * @return the number of bytes evicted, or zero if no progress was made.
     */
    private synchronized long evictBatch() 
        throws DatabaseException {

        INList inList = envImpl.getInMemoryINs();

        inList.latchMajor();
        nNodesSelectedThisRun = 0;
        nNodesEvictedThisRun = 0;
        nNodesScannedThisRun = 0;
        nBINsStrippedThisRun = 0;
        nEvictPasses++;
        int nBatchSets = 0;

        assert evictProfile.clear(); // intentional side effect

        boolean finished = false;
        int inListStartSize = inList.getSize();
        long evictBytes = 0;
        try {
        
            /*
             * Keep evicting until we've evicted enough or we've visited all
             * the nodes once. Each iteration of the while loop is called an
             * eviction batch.
             *
             * In order to prevent endless evicting and not keep the INList
             * major latch for too long, limit this run to one pass over the IN
             * list.
             */
            int maxVisitedNodes = inListStartSize;
            while ((evictBytes < requiredEvictBytes) &&
                   (nNodesScannedThisRun <= maxVisitedNodes)) {
                SortedSet targetNodes = selectINSet(inList);
                if (targetNodes == null) {
                    break;
                } else {
                    evictBytes = evict(inList, targetNodes, evictBytes);
                }
                nBatchSets++;

                assert nBatchSets < 1000;  // ensure we come out of this loop
            }
            finished = true;
        } finally {
            inList.releaseMajorLatch();
	    Tracer.trace(detailedTraceLevel, envImpl,
                         "Evictor: pass=" + nEvictPasses +
                         " finished=" + finished +
                         " requiredEvictBytes=" +
                         formatter.format(requiredEvictBytes) +
                         " inListSize=" + inListStartSize +
                         " nNodesScanned=" + nNodesScannedThisRun +
                         " nNodesSelected=" + nNodesSelectedThisRun +
                         " nEvicted=" + nNodesEvictedThisRun +
                         " nBINsStripped=" + nBINsStrippedThisRun +
                         " nBatchSets=" + nBatchSets);
            
            if (DEBUG) {
                StringBuffer dump = new StringBuffer();
                assert evictProfile.dump(dump);
                Tracer.trace(detailedTraceLevel, envImpl, dump.toString());
            }
        }
        assert Latch.countLatchesHeld() == 0: "latches held = " +
            Latch.countLatchesHeld();
        return evictBytes;
    }

    /**
     * Return true if eviction should happen.
     */
    boolean isRunnable(boolean force)
        throws DatabaseException {

        boolean doRun = false;
        MemoryBudget mb = envImpl.getMemoryBudget();
        long currentUsage  = mb.getCacheMemoryUsage();
        long maxMem = mb.getTreeBudget();
        if (force) {
            doRun = true;
        } else {
            doRun = ((currentUsage - maxMem) > 0);
        }

        /* If running, figure out how much to evict. */
        if (doRun) {
            int floor = envImpl.getConfigManager().
                getInt(EnvironmentParams.EVICTOR_USEMEM_FLOOR);
            long floorBytes = ((maxMem * floor) / 100);
            requiredEvictBytes = currentUsage - floorBytes;
        }

        /* 
         * This trace message is expensive, only generate if tracing at this
         * level is enabled.
         */
        Logger logger = envImpl.getLogger();
        if (logger.isLoggable(detailedTraceLevel)) {

            /* 
             * Generate debugging output. Note that Runtime.freeMemory
             * fluctuates over time as the JVM grabs more memory, so you really
             * have to do totalMemory - freeMemory to get stack usage.  (You
             * can't get the concept of memory available from free memory.)
             */
            Runtime r = Runtime.getRuntime();
            long totalBytes = r.totalMemory();
            long freeBytes= r.freeMemory();
            long usedBytes = r.totalMemory() - r.freeMemory(); 
            Tracer.trace(detailedTraceLevel, envImpl,
                         " doRun=" + doRun +
                         " JEusedBytes=" +
                         formatter.format(currentUsage) +
                         " requiredEvict=" + 
                         formatter.format(requiredEvictBytes) +
                         " JVMtotalBytes= " + formatter.format(totalBytes) +
                         " JVMfreeBytes= " + formatter.format(freeBytes) +
                         " JVMusedBytes= " + formatter.format(usedBytes));
        }

        return doRun;
    }

    /**
     * Select a set of nodes to evict.  This is public because a unit test
     * needs access to it.
     *
     * FindBugs will whine about nNodesScannedThisRun not being synchronized
     * but it's ok.  When this is called from outside, it's only in a test.
     */
    public SortedSet selectINSet(INList inList)
        throws DatabaseException {

        /*
         * Determine size of scan and eviction batches based on current
         * INList size.
         */
        int inListTotalSize = inList.getSize();

        long batchNodes = (inListTotalSize * nodeScanPercentage) / 100;
        if (batchNodes == 0) {
            batchNodes = 1;
        }
        nNodesScannedThisRun += batchNodes;
        nNodesScanned += batchNodes;
        
        /*
         * Plan on evicting only a portion of this. 
         */
        long nodesToEvict =
	    (batchNodes * evictionBatchPercentage) / 100;

        if (nodesToEvict == 0) {
            nodesToEvict = 1;
        }

        if (nextNode == null && inListTotalSize > 0) {
            nextNode = inList.first();
        }

        if (nextNode == null) {
            return null;
        }

        /*
         * Use a second sorted set (targetNodes) to store the eviction
         * candidates.  Rather than being sorted on NodeId, it is sorted on
         * generation count.  Once the set has nodesToEvict IN's in it, we
         * check new IN's that we encounter against the current maximum
         * generation in the eviction candidates.  If the IN we're checking has
         * a generation less than the current max, then the IN with the current
         * max is removed from the candidate list (it's the last() element),
         * and the new one added.
         *
         * If during our scan we run off the end of the list, just go back to
         * the beginning.
         */
        SortedSet targetNodes = new TreeSet(inGenerationComparator);
        long currentMax = Long.MAX_VALUE;
        SortedSet tailSet = inList.tailSet(nextNode);
        Iterator iter = tailSet.iterator();

        for (int i = 0; i < batchNodes; i++) {
            if (iter.hasNext()) {
                IN in = (IN) iter.next();

                DatabaseImpl db = in.getDatabase();

                if (db == null || db.getIsDeleted()) {
                    /* Database was deleted.  Remove it from the inlist. */
                    iter.remove();
                    continue;
                }

                if (db.getId().equals(DbTree.ID_DB_ID)) {

                    /*
                     * Don't evict the DatabaseImpl Id Mapping Tree db 0), both
                     * (for object identity reasons and * because the id mappng
                     * (tree should stay cached.
                     */
                    continue;
                }

                if (in.isEvictable()) {
                    long inGen = in.getGeneration();

                    if (targetNodes.size() < nodesToEvict) {
                        targetNodes.add(in);
                        continue;
                    }

                    currentMax = ((IN) targetNodes.last()).getGeneration();
                    if (inGen < currentMax) {
                        targetNodes.remove(targetNodes.last());
                        targetNodes.add(in);
                        currentMax =
                            ((IN) targetNodes.last()).getGeneration();
                    }
                }
            } else {
                /* We wrapped around in the list. */
                nextNode = inList.first();
                tailSet = inList.tailSet(nextNode);
                iter = tailSet.iterator();
            }
        }

        /*
         * At the end of the scan, look at the next element in the INList and
         * put it in nextNode for the next time we scan the INList.
         */
        if (iter.hasNext()) {
            nextNode = (IN) iter.next();
        } else {
            nextNode = inList.first();
        }

        nNodesSelectedThisRun += targetNodes.size();
        nNodesSelected += targetNodes.size();
        return targetNodes;
    }

    /**
     * Evict this set of nodes. The nodes are always INs (or a subclass).
     * @return number of bytes evicted.
     */
    private long evict(INList inList,
		       SortedSet targetNodes,
                       long alreadyEvicted)
        throws DatabaseException {
        
        Iterator iter = targetNodes.iterator();
        long currentEvictBytes = alreadyEvicted;

	boolean envIsReadOnly = envImpl.isReadOnly();

        /*
         * Non-BIN INs are evicted by detaching them from their parent.  BINS
         * have a two-step eviction process. First we try to get away with
         * merely detaching all their resident LN targets. If they have no
         * resident LNs and we have to get rid of the actual node, then we
         * follow the "regular" detach-from-parent routine.
         *
         * We do our evictions from the bottom of the tree upwards, so we don't
         * risk fetching in just-evicted nodes in the process of evicting.
         * 
         * evictTargets orders the eviction candidates by level.
         */
        LevelOrderedINMap evictTargets = new LevelOrderedINMap();

        while (iter.hasNext() && (currentEvictBytes < requiredEvictBytes)) {
            IN target = (IN) iter.next();
            boolean addToTargetSet = true;

            if (target instanceof BIN) {

                /* 
                 * Strip any resident LN targets right now. No need to dirty
                 * the BIN, the targets are not persistent data.
                 */
                target.latch();
                try {
                    long evictBytes = ((BIN) target).evictLNs();
                    if (evictBytes > 0) {
                        addToTargetSet = false;
                        nBINsStrippedThisRun++;
                        nBINsStripped++;
                        currentEvictBytes += evictBytes;
                    }                        
                } finally {
                    target.releaseLatch();
                }
            } 

            if (addToTargetSet) {
                evictTargets.putIN(target);
            }
        }
            
        LogManager logManager = envImpl.getLogManager();

        while ((evictTargets.size() > 0) &&
               (currentEvictBytes < requiredEvictBytes)) {

            /* 
             * Work on one level's worth of nodes, ascending up the tree, so we
             * don't have to fault in just-evicted nodes in the act of doing
             * evictions.
             */
            Integer currentLevel = (Integer) evictTargets.firstKey();
            Set inSet = (Set) evictTargets.remove(currentLevel);
            iter = inSet.iterator();
                
            /* Evict all these nodes */
            while (iter.hasNext() &&
                   (currentEvictBytes < requiredEvictBytes)) {
                IN child = (IN) iter.next();
                child.latch();
		try {
		    if (child.isEvictable()) {
			Tree tree = child.getDatabase().getTree();
			/* getParentINForChildIN unlatches child. */
			SearchResult result =
			    tree.getParentINForChildIN(child, true);
			if (result.exactParentFound) {
			    currentEvictBytes +=
				evictIN(child, result.parent, result.index,
					inList, targetNodes, logManager,
					envIsReadOnly);
			}
		    } else {
			child.releaseLatch();
		    }
		} finally {
		    if (child.getLatch().isOwner()) {
			child.releaseLatch();
		    }
		}
            }
        }

        return currentEvictBytes;
    }

    /**
     * Evict an IN. Dirty nodes are logged before they're evicted. Inlist is
     * latched with the major latch by the caller.
     */
    private long evictIN(IN child,
                         IN parent,
                         int index,
                         INList inlist,
                         SortedSet targetNodes,
                         LogManager logManager,
			 boolean envIsReadOnly)
        throws DatabaseException {

        long evictBytes = 0;
        try {
            assert parent.getLatch().isOwner();

            long oldGenerationCount = child.getGeneration();
            
            /* 
             * Get a new reference to the child, in case the reference
             * saved in the selection list became out of date because of
             * changes to that parent.
             */
            IN renewedChild = (IN) parent.getEntry(index).getTarget();
            
            if ((renewedChild != null) &&
                (renewedChild.getGeneration() <= oldGenerationCount)) {

                renewedChild.latch();
                try {
                    if (renewedChild.isEvictable()) {

                        /* 
                         * Log the child if dirty and env is not r/o.
                         * Remove from IN list.
                         */
                        DbLsn renewedChildLsn = null;
                        boolean newChildLsn = false;
                        if (renewedChild.getDirty()) {
                            if (!envIsReadOnly) {
                                renewedChildLsn =
                                    renewedChild.log(logManager);
                                newChildLsn = true;
                            }
                        } else {
                            renewedChildLsn =
                                parent.getEntry(index).getLsn();
                        }

                        if (renewedChildLsn != null) {
                            /* Take this off the inlist. */
                            inlist.removeLatchAlreadyHeld(renewedChild);

                            /* 
                             * This may return false since renewedChild
                             * doesn't have to be on targetNodes.
                             */
                            targetNodes.remove(renewedChild);

                            evictBytes = renewedChild.getInMemorySize();
                            if (newChildLsn) {

                                /* 
                                 * Update the parent so its reference is
                                 * null and it has the proper lsn.
                                 */
                                parent.updateEntry
                                    (index, null, renewedChildLsn);
                            } else {

                                /*
                                 * Null out the reference, but don't dirty
                                 * the node since only the reference
                                 * changed.
                                 */
                                parent.updateEntry(index, (Node) null);
                            }

                            /* Stats */
                            nNodesEvictedThisRun++;
                            nNodesEvicted++;

                            /* Intentional side effect */
                            assert evictProfile.count(renewedChild);
                        }
                    }
                } finally {
                    renewedChild.releaseLatch();
                }
            }
        } finally {
            parent.releaseLatch();
        }

        return evictBytes;
    }

    private void dumpSortedSet(SortedSet ins) {
        if (ins != null) {
            Iterator iter = ins.iterator();
            while (iter.hasNext()) {
                IN theIN = (IN) iter.next();
                System.out.print(" " + theIN.shortClassName() + " " +
                                 theIN.getLevel() + "/" +
                                 theIN.getDatabase().getId() + "/" +
                                 theIN.getNodeId() + "/" +
                                 theIN.getGeneration());
            }
            System.out.println("");
        }
    }

    /* For debugging */
    static public class EvictProfile {
        private HashMap evictCounts = new HashMap();
        
        private int nEvictedBINs = 0;

        /* Remember that this node was evicted. */
        boolean count(IN target) {
            if (target instanceof BIN) {
                nEvictedBINs++;
            }
            Long key = new Long(target.getNodeId());
            Integer count = (Integer) evictCounts.get(key);
            if (count == null) {
                evictCounts.put(key, new Integer(1));
            } else {
                evictCounts.put(key, new Integer(1+ count.intValue()));
            }
            return true;
        }

        boolean dump(StringBuffer sb) {
            Iterator iter = evictCounts.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry entry = (Map.Entry) iter.next();
                Long nodeId = (Long) entry.getKey();
                Integer count = (Integer) entry.getValue();

                sb.append(" nid=").append(nodeId).append(" / ").append(count);
            }
            sb.append("nEvictedBIN=").append(nEvictedBINs);
            return true;
        }

        public boolean clear() {
            evictCounts.clear();
            nEvictedBINs = 0;
            return true;
        }

        /**
         * @return
         */
        public int getNEvictedBINs() {
            return nEvictedBINs;
        }
    }
}
