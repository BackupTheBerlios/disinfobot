/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: Checkpointer.java,v 1.1 2004/11/22 18:27:59 kate Exp $
*/

package com.sleepycat.je.recovery;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.logging.Level;

import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.cleaner.Cleaner;
import com.sleepycat.je.cleaner.TrackedFileSummary;
import com.sleepycat.je.cleaner.UtilizationProfile;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.DbTree;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.EnvironmentStatsInternal;
import com.sleepycat.je.dbi.INList;
import com.sleepycat.je.dbi.MemoryBudget;
import com.sleepycat.je.log.LogManager;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.ChildReference;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.tree.Node;
import com.sleepycat.je.tree.SearchResult;
import com.sleepycat.je.tree.Tree;
import com.sleepycat.je.tree.WithRootLatched;
import com.sleepycat.je.utilint.DaemonThread;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.PropUtil;
import com.sleepycat.je.utilint.Tracer;

/**
 * The Checkpointer looks through the tree for internal nodes that must be
 * flushed to the log. Checkpoint flushes must be done in ascending order
 * from the bottom of the tree up.
 */
public class Checkpointer extends DaemonThread {

    private EnvironmentImpl envImpl;

    /* Checkpoint sequence, initialized at recovery. */
    private long checkpointId;  

    /* 
     * How much the log should grow between checkpoints. If 0, we're
     * using time based checkpointing.
     */
    private long logSizeBytesInterval;
    private long logFileMax;
    private long timeInterval;
    private long lastCheckpointMillis;

    /* Stats */
    private int nCheckpoints;
    private DbLsn lastFirstActiveLsn;
    private DbLsn lastCheckpointStart;
    private DbLsn lastCheckpointEnd;

    private int nFullINFlush;
    private int nFullBINFlush;
    private int nDeltaINFlush;
    private int nFullINFlushThisRun;
    private int nDeltaINFlushThisRun;

    private int highestFlushLevel;

    public Checkpointer(EnvironmentImpl envImpl,
                        long waitTime,
                        String name) 
        throws DatabaseException {

        super(waitTime, name, envImpl);
        this.envImpl = envImpl;
        logSizeBytesInterval = 
            envImpl.getConfigManager().getLong(
                          EnvironmentParams.CHECKPOINTER_BYTES_INTERVAL);
        logFileMax = 
            envImpl.getConfigManager().getLong(EnvironmentParams.LOG_FILE_MAX);
        nCheckpoints = 0;
        timeInterval = waitTime;
        lastCheckpointMillis = 0;
    }

    /**
     * Figure out the wakeup period. Supplied through this static method
     * because we need to pass wakeup period to the superclass and
     * need to do the calcuation outside this constructor.
     */
    public static long getWakeupPeriod(DbConfigManager configManager) 
        throws IllegalArgumentException, DatabaseException {

        long wakeupPeriod = PropUtil.microsToMillis(configManager.getLong
            (EnvironmentParams.CHECKPOINTER_WAKEUP_INTERVAL));
        long bytePeriod = configManager.getLong
            (EnvironmentParams.CHECKPOINTER_BYTES_INTERVAL);

        /* Checkpointing period must be set either by time or by log size. */
        if ((wakeupPeriod == 0) && (bytePeriod == 0)) {
            throw new IllegalArgumentException(
             EnvironmentParams.CHECKPOINTER_BYTES_INTERVAL.getName() +
             " and " +
             EnvironmentParams.CHECKPOINTER_WAKEUP_INTERVAL.getName() +
             " cannot both be 0. ");
        }

        /*
         * Checkpointing by log size takes precendence over time based period
         */
        if (bytePeriod == 0) {
            return wakeupPeriod;
        } else {
            return 0;
        }
    }

    /**
     * Set checkpoint id -- can only be done after recovery.
     */
    synchronized public void setCheckpointId(long lastCheckpointId) {
        checkpointId = lastCheckpointId;
    }

    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("<Checkpointer name=\"").append(name).append("\"/>");
        return sb.toString();
    }

    /**
     * Load stats.
     */
    public void loadStats(StatsConfig config, EnvironmentStatsInternal stat) 
        throws DatabaseException {

        stat.setNCheckpoints(nCheckpoints);
        stat.setLastCheckpointStart(lastCheckpointStart);
        stat.setLastCheckpointEnd(lastCheckpointEnd);
        stat.setLastCheckpointId(checkpointId);
        stat.setNFullINFlush(nFullINFlush);
        stat.setNFullBINFlush(nFullBINFlush);
        stat.setNDeltaINFlush(nDeltaINFlush);
        
        if (config.getClear()) {
            nCheckpoints = 0;
            nFullINFlush = 0;
            nFullBINFlush = 0;
            nDeltaINFlush = 0;
        }
    }
    
    /**
     * @return the first active lsn point of the last completed checkpoint.
     * If no checkpoint has run, return null.
     */
    public DbLsn getFirstActiveLsn() {
        return lastFirstActiveLsn;
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
            (EnvironmentParams.CHECKPOINTER_RETRY);
    }

    /**
     * Called whenever the DaemonThread wakes up from a sleep.  
     */
    protected void onWakeup()
        throws DatabaseException {

        if (envImpl.isClosed()) {
            return;
        }

        doCheckpoint(CheckpointConfig.DEFAULT,
                     true,  // allowDeltas
                     false, // flushAll
                     false, // deleteAllCleanedFiles
                     "daemon");
    }

    /**
     * Determine whether a checkpoint should be run.
     * 1. If the force parameter is specified, always checkpoint. 
     * 2. If the config object specifies time or log size, use that.
     * 3. If the environment is configured to use log size based checkpointing,
     * check the log.
     * 4. Lastly, use time based checking.
     */
    private boolean isRunnable(CheckpointConfig config)
        throws DatabaseException {

        /* Figure out if we're using log size or time to determine interval.*/
        long useBytesInterval = 0;
        long useTimeInterval = 0;
        DbLsn nextLsn = null;
        try {
            if (config.getForce()) {
                return true;
            } else if (config.getKBytes() != 0) {
                useBytesInterval = config.getKBytes() << 10;
            } else if (config.getMinutes() != 0) {
                // convert to millis
                useTimeInterval = config.getMinutes() * 60 * 1000;
            } else if (logSizeBytesInterval != 0) {
                useBytesInterval = logSizeBytesInterval;
            } else {
                useTimeInterval = timeInterval;
            }

            /* 
             * If our checkpoint interval is defined by log size, check
             * on how much log has grown since the last checkpoint.
             */
            if (useBytesInterval != 0) {
                nextLsn = envImpl.getFileManager().getNextLsn();
                if (nextLsn.getNoCleaningDistance(lastCheckpointEnd,
                                                  logFileMax) >=
                    useBytesInterval) {
                    return true;
                } else {
                    return false;
                }
            } else if (useTimeInterval != 0) {

                /* 
                 * Our checkpoint is determined by time.  If enough
                 * time has passed and some log data has been written,
                 * do a checkpoint.
                 */
                DbLsn lastUsedLsn = envImpl.getFileManager().getLastUsedLsn();
                if (((System.currentTimeMillis() - lastCheckpointMillis) >=
                     useTimeInterval) &&
                    (lastUsedLsn.compareTo(lastCheckpointEnd) != 0)) {
                    return true;
                } else {
                    return false;
                }
            } else {
                return false;
            }
        } finally {
            StringBuffer sb = new StringBuffer();
            sb.append("size interval=").append(useBytesInterval);
            if (nextLsn != null) {
                sb.append(" nextLsn=").append(nextLsn.getNoFormatString());
            }
            if (lastCheckpointEnd != null) {
                sb.append(" lastCkpt=");
                sb.append(lastCheckpointEnd.getNoFormatString());
            }
            sb.append(" time interval=").append(useTimeInterval);
            sb.append(" force=").append(config.getForce());
            
            Tracer.trace(Level.FINEST,
                         envImpl, 
                         sb.toString());
        }
    }

    /**
     * The real work to do a checkpoint. This may be called by the checkpoint
     * thread when waking up, or it may be invoked programatically through the
     * api.
     *
     * Lock out the evictor during checkpoint.  See SR 10249.
     * 
     * @param allowDeltas if true, this checkpoint may opt to log BIN deltas
     *       instead of the full node.
     * @param flushAll if true, this checkpoint must flush all the way to
     *       the top of the dbtree, instead of stopping at the highest level
     *       last modified.
     * @param deleteAllCleanedFiles if true, delete all cleaned files (ignore
     *       the minimum file threshold); used during Environment.close.
     * @param invokingSource a debug aid, to indicate who invoked this 
     *       checkpoint. (i.e. recovery, the checkpointer daemon, the cleaner,
     *       programatically)
     */
    public void doCheckpoint(CheckpointConfig config,
			     boolean allowDeltas,
			     boolean flushAll,
			     boolean deleteAllCleanedFiles,
			     String invokingSource) 
        throws DatabaseException {

	synchronized (envImpl.getEvictor()) {
	    doCheckpointInternal(config, allowDeltas,
				 flushAll, deleteAllCleanedFiles,
                                 invokingSource);
	}
    }

    /**
     * Helper for doCheckpoint.
     * Same args only this is called with the evictor locked out.
     */
    private synchronized void doCheckpointInternal(CheckpointConfig config,
						   boolean allowDeltas,
						   boolean flushAll,
                                                   boolean
                                                   deleteAllCleanedFiles,
						   String invokingSource) 
        throws DatabaseException {

	if (!isRunnable(config)) {
	    return;
	}

        /*
         * If there are cleaned files to be deleted, flush an extra level to
         * write out the parents of cleaned nodes.  This ensures that the
         * no node will contain the LSN of a cleaned files.
         */
        boolean flushExtraLevel = false;
        Set cleanedFiles = null;
        Cleaner cleaner = envImpl.getCleaner();
        if (cleaner != null) {
            cleanedFiles = cleaner.getCleanedFiles(deleteAllCleanedFiles);
            if (cleanedFiles != null) {
                flushExtraLevel = true;
            }
        }

        lastCheckpointMillis = System.currentTimeMillis();
        resetPerRunCounters();
        LogManager logManager = envImpl.getLogManager();

        /* Get the next checkpoint id. */
        checkpointId++;
        nCheckpoints++;
        boolean success = false;
        boolean traced = false;
        try {
            /* Log the checkpoint start. */
            CheckpointStart startEntry =
		new CheckpointStart(checkpointId, invokingSource);
            DbLsn checkpointStart = logManager.log(startEntry);

            /* 
             * Remember the first active lsn -- before this position in the
             * log, there are no active transactions at this point in time.
             */
            DbLsn firstActiveLsn =
                envImpl.getTxnManager().getFirstActiveLsn();

	    if (firstActiveLsn != null &&
		checkpointStart.compareTo(firstActiveLsn) < 0) {
		firstActiveLsn = checkpointStart;
	    }

            /* Flush IN nodes. */
            flushDirtyNodes(flushAll, allowDeltas, flushExtraLevel);

            /*
             * Flush utilization info AFTER flushing IN nodes to reduce the
             * inaccuracies caused by the sequence FileSummaryLN-LN-BIN.
             */
            flushUtilizationInfo();

            /* Log the checkpoint end. */
            if (firstActiveLsn == null) {
                firstActiveLsn = checkpointStart;
            }
            CheckpointEnd endEntry =
                new CheckpointEnd(invokingSource,
                                  checkpointStart,
                                  envImpl.getRootLsn(),
                                  firstActiveLsn,
                                  Node.getLastId(),
                                  envImpl.getDbMapTree().getLastDbId(),
                                  envImpl.getTxnManager().getLastTxnId(),
                                  checkpointId);

            /* 
             * Log checkpoint end and update state kept about the last
             * checkpoint location. Send a trace message *before* the
             * checkpoint end log entry. This is done  so that the  normal
             * trace  message doesn't affect the time-based isRunnable()
             * calculation, which only issues a checkpoint if a log record
             * has been written since the last checkpoint.
             */
            trace(envImpl, invokingSource, true);
            traced = true;

            /*
             * Always flush to ensure that cleaned files are not referenced,
             * and to ensure that this checkpoint is not wasted if we crash.
             */
            lastCheckpointEnd = logManager.logForceFlush(endEntry);
            lastFirstActiveLsn = firstActiveLsn;
            lastCheckpointStart = checkpointStart;

            success = true;

            if (cleaner != null && cleanedFiles != null) {
                cleaner.deleteCleanedFiles(cleanedFiles);
            }
        } catch (DatabaseException e) {
            Tracer.trace(envImpl, "Checkpointer", "doCheckpoint",
                         "checkpointId=" + checkpointId, e);
            throw e;
        } finally {
            if (!traced) {
                trace(envImpl, invokingSource, success);
            }
        }
    }

    private void trace(EnvironmentImpl envImpl,
                       String invokingSource, boolean success ) {
        StringBuffer sb = new StringBuffer();
        sb.append("Checkpoint ").append(checkpointId);
        sb.append(": source=" ).append(invokingSource);
        sb.append(" success=").append(success);
        sb.append(" nFullINFlushThisRun=").append(nFullINFlushThisRun);
        sb.append(" nDeltaINFlushThisRun=").append(nDeltaINFlushThisRun);
        Tracer.trace(Level.INFO, envImpl, sb.toString());
    }

    /**
     * Flush a FileSummaryLN node for each TrackedFileSummary that is
     * currently active.  Tell the UtilizationProfile about the updated file
     * summary.
     */
    private void flushUtilizationInfo()
        throws DatabaseException {

        if (!DbInternal.getCheckpointUP
                (envImpl.getConfigManager().getEnvironmentConfig())) {
            return; // only disabled for unittests
        }
        
        UtilizationProfile profile = envImpl.getUtilizationProfile();

        TrackedFileSummary[] activeFiles =
            envImpl.getUtilizationTracker().getTrackedFiles();

        for (int i = 0; i < activeFiles.length; i += 1) {
            TrackedFileSummary trackedFile = activeFiles[i];
            profile.putFileSummary(trackedFile);
        }
    }

    /**
     * Flush the nodes in order, from the lowest level to highest
     * level.  As a flush dirties its parent, add it to the dirty map,
     * thereby cascading the writes up the tree. If flushAll wasn't
     * specified, we need only cascade up to the highest level that
     * existed before the checkpointing started.
     *
     * Note that all but the top level INs and the BINDeltas are
     * logged provisionally. That's because we don't need to process
     * lower INs because the higher INs will end up pointing at them.
     */
    private void flushDirtyNodes(boolean flushAll,
				 boolean allowDeltas,
                                 boolean flushExtraLevel)
        throws DatabaseException {

        LogManager logManager = envImpl.getLogManager();

        SortedMap dirtyMap = selectDirtyINs(flushAll, flushExtraLevel);

        while (dirtyMap.size() > 0) {

            /* Work on one level's worth of nodes in ascending level order. */
            Integer currentLevel = (Integer) dirtyMap.firstKey();
            boolean logProvisionally =
                (currentLevel.intValue() != highestFlushLevel);

            Set nodeSet = (Set) dirtyMap.get(currentLevel);
            Iterator iter = nodeSet.iterator();
                
            /* Flush all those nodes */
            while (iter.hasNext()) {
                IN target = (IN) iter.next();
                target.latch();
                boolean triedToFlush = false;

                /* 
                 * Only flush the ones that are still dirty -- some
                 * may have been written out by the evictor. Also
                 * check if the db is still valid -- since INs of
                 * deleted databases are left on the in-memory tree
                 * until the evictor lazily clears them out, there may
                 * be dead INs around.
                 */
                if (target.getDirty() &&
                    (!target.getDatabase().getIsDeleted())) {
                    flushIN(target, logManager, dirtyMap,
                            logProvisionally, allowDeltas);
                    triedToFlush = true;
                } else {
                    target.releaseLatch();
                }

                Tracer.trace(Level.FINE, envImpl,
			     "Checkpointer: node=" +
			     target.getNodeId() +
			     " level=" + 
			     Integer.toHexString(target.getLevel()) +
			     " flushed=" + triedToFlush);
            }

            /* We're done with this level. */
            dirtyMap.remove(currentLevel);

            /* We can stop at this point. */
            if (currentLevel.intValue() == highestFlushLevel) {
                break;
            }
        }
    }

    /* 
     * Scan the INList for all dirty INs. Arrange them in level sorted 
     * map for level ordered flushing.
     */
    private SortedMap selectDirtyINs(boolean flushAll,
                                     boolean flushExtraLevel) 
        throws DatabaseException {

        SortedMap newDirtyMap = new TreeMap();

        INList inMemINs = envImpl.getInMemoryINs();
        inMemINs.latchMajor();

        /* 
	 * Opportunistically recalculate the environment wide memory
	 * count.  Incurs no extra cost because we're walking the IN
	 * list anyway.  Not the best in terms of encapsulation as
	 * prefereably all memory calculations are done in
	 * MemoryBudget, but done this way to avoid any extra
	 * latching.
	 */
        long totalSize = 0;
        MemoryBudget mb = envImpl.getMemoryBudget();

        try {
            Iterator iter = inMemINs.iterator();
            while (iter.hasNext()) {
                IN in = (IN) iter.next();
                in.latch();
                totalSize = mb.accumulateNewUsage(in, totalSize);
                boolean isDirty = in.getDirty();
                in.releaseLatch();
                if (isDirty) {
                    Integer level = new Integer(in.getLevel());
                    Set dirtySet;
                    if (newDirtyMap.containsKey(level)) {
                        dirtySet = (Set) newDirtyMap.get(level);
                    } else {
                        dirtySet = new HashSet();
                        newDirtyMap.put(level, dirtySet);
                    }
                    dirtySet.add(in);
                }
            }

            // Later release: refresh env count.
            // mb.refreshCacheMemoryUsage(totalSize);

            /* 
             * If we're flushing all for cleaning, we must flush to
             * the point that there are no nodes with LSNs in the
             * cleaned files.  We could figure this out by perusing
             * every node to see what children it has, but that's so
             * expensive that instead we'll flush to the root.
             */
            if (newDirtyMap.size() > 0) {
                if (flushAll) {
                    highestFlushLevel =
			envImpl.getDbMapTree().getHighestLevel();
                } else {
                    highestFlushLevel =
                        ((Integer) newDirtyMap.lastKey()).intValue();
                    if (flushExtraLevel) {
                        highestFlushLevel += 1;
                    }
                }
            }

        } finally {
            inMemINs.releaseMajorLatchIfHeld();
        }

        return newDirtyMap;
    }
    
    /** 
     * Flush the target IN.
     */
    private void flushIN(IN target,
                         LogManager logManager,
                         Map dirtyMap,
                         boolean logProvisionally,
                         boolean allowDeltas)
        throws DatabaseException {

        DatabaseImpl db = target.getDatabase();
        Tree tree = db.getTree();
        boolean targetWasRoot = false;

        if (target.isDbRoot()) {
            /* We're trying to flush the root. */
            target.releaseLatch();
            RootFlusher flusher = new RootFlusher(db, logManager, target);
            tree.withRootLatched(flusher);
            boolean flushed = flusher.getFlushed();

            /* 
             * We have to check if the root split between target.releaseLatch
             * and the execution of the root flusher. If it did split, this
             * target has to get handled like a regular node.
             */
            targetWasRoot = flusher.stillRoot();
            
            /* 
             * Update the tree's owner, whether it's the env root or the
             * dbmapping tree.
             */
            if (flushed) {
                DbTree dbTree = db.getDbEnvironment().getDbMapTree();
                dbTree.modifyDbRoot(db);
                nFullINFlushThisRun++;
                nFullINFlush++;
            }
            if (!targetWasRoot) {
                /* 
                 * re-latch for another attempt, now that this is no longer
                 * the root.
                 */
                target.latch();
            }
        }

        if (!targetWasRoot) {
            SearchResult result = tree.getParentINForChildIN(target, true);

            /* 
             * Found a parent, do the flush. If no parent found, the
             * compressor deleted this item before we got to processing it.
             */
            if (result.exactParentFound) {
                try {
                    ChildReference entry =
                        result.parent.getEntry(result.index);
                    IN renewedTarget =
                        (IN) entry.fetchTarget(db, result.parent);
                    renewedTarget.latch();
                    DbLsn newLsn = null;
                    try {

                        /* Still dirty? */
                        if (renewedTarget.getDirty()) {
                            if (allowDeltas) {
                                newLsn = renewedTarget.logAllowDeltas
                                    (logManager, logProvisionally);
                                if (newLsn == null) {
                                    nDeltaINFlushThisRun++;
                                    nDeltaINFlush++;
                                }
                            } else {
                                newLsn = renewedTarget.log(logManager,
                                                           logProvisionally);
                            }
                        }
                    } finally {
                        renewedTarget.releaseLatch();
                    }
                        
                    /* Update parent if logging occurred */
                    if (newLsn != null) {
                        nFullINFlushThisRun++;
                        nFullINFlush++;
                        if (renewedTarget instanceof BIN) {
                            nFullBINFlush++;
                        }
                        result.parent.updateEntry(result.index, newLsn);
                        addToDirtyMap(dirtyMap, result.parent);
                    }
                } finally {
                    result.parent.releaseLatch();
                }
            }
        }
    }

    /*
     * RootFlusher lets us write out the root IN within the root latch.
     */
    private static class RootFlusher implements WithRootLatched {
        private DatabaseImpl db;
        private boolean flushed;
        private boolean stillRoot;
        private LogManager logManager;
        private IN target;

        RootFlusher(DatabaseImpl db, LogManager logManager, IN target) {
            this.db = db;
            flushed = false;
            this.logManager = logManager;
            this.target = target;
            stillRoot = false;
        }

        /**
         * @return true if the in-memory root was replaced.
         */
        public IN doWork(ChildReference root) 
            throws DatabaseException {

	    if (root == null) {
		return null;
	    }
            IN rootIN = (IN) root.fetchTarget(db, null);
            rootIN.latch();
            try {
                if (rootIN.getNodeId() == target.getNodeId()) {

                    /* 
		     * stillRoot handles race condition where root
		     * splits after target's latch is release.
                     */
                    stillRoot = true;
                    if (rootIN.getDirty()) {
                        DbLsn newLsn = rootIN.log(logManager);
                        root.setLsn(newLsn);
                        flushed = true;
                    }
                }
            } finally {
                rootIN.releaseLatch();
            }                    
            return null;
        }

        boolean getFlushed() {
            return flushed;
        }

        boolean stillRoot() {
            return stillRoot;
        }
    }

    /**
     * Add a node to the dirty map. The dirty map is keyed by level (Integers)
     * and holds sets of IN references.
     */
    private void addToDirtyMap(Map dirtyMap, IN in) {
        Integer inLevel = new Integer(in.getLevel());
        Set inSet = (Set) dirtyMap.get(inLevel);
        
        /* If this level doesn't exist in the map yet, make a new entry. */
        if (inSet == null) {
            inSet = new HashSet();
            dirtyMap.put(inLevel, inSet);
        }    
        
        /* Add to the set. */
        inSet.add(in);
    }

    /**
     * Reset per-run counters.
     */
    private void resetPerRunCounters() {
        nFullINFlushThisRun = 0;
        nDeltaINFlushThisRun = 0;
    }
}
