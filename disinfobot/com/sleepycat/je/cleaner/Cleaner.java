/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: Cleaner.java,v 1.1 2004/11/22 18:27:58 kate Exp $
*/

package com.sleepycat.je.cleaner;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.DbTree;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.EnvironmentStatsInternal;
import com.sleepycat.je.latch.Latch;
import com.sleepycat.je.log.CleanerFileReader;
import com.sleepycat.je.log.FileManager;
import com.sleepycat.je.log.LogFileNotFoundException;
import com.sleepycat.je.log.LogManager;
import com.sleepycat.je.log.entry.LNLogEntry;
import com.sleepycat.je.log.entry.LogEntry;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.ChildReference;
import com.sleepycat.je.tree.DIN;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.tree.Key;
import com.sleepycat.je.tree.LN;
import com.sleepycat.je.tree.Node;
import com.sleepycat.je.tree.SearchResult;
import com.sleepycat.je.tree.Tree;
import com.sleepycat.je.tree.TreeLocation;
import com.sleepycat.je.tree.WithRootLatched;
import com.sleepycat.je.txn.BasicLocker;
import com.sleepycat.je.txn.LockGrantType;
import com.sleepycat.je.txn.Locker;
import com.sleepycat.je.utilint.DaemonThread;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.PropUtil;
import com.sleepycat.je.utilint.Tracer;

/**
 * The Cleaner is responsible for effectively garbage collecting the JE log.
 * It looks through log files and locates log records (IN's and LN's of all
 * flavors) that are superceded by later versions.  Those that are "current"
 * are propagated to a newer log file so that older log files can be deleted.
 */
public class Cleaner extends DaemonThread {
    /* From cleaner */
    private static final String CLEAN_IN = "CleanIN:";
    private static final String CLEAN_LN = "CleanLN:";
    private static final String CLEAN_DELTA = "CleanDelta:";

    private EnvironmentImpl env;
    private long lockTimeout;
    private Set filesToDelete;

    /* Cumulative counters. */
    private int nCleanerRuns = 0;
    private int nCleanerDeletions = 0;
    private int nINsCleaned = 0;
    private int nINsMigrated = 0;
    private int nLNsCleaned = 0;
    private int nLNsDead = 0;
    private int nLNsLocked = 0;
    private int nLNsMigrated = 0;
    private int nDeltasCleaned = 0;
    private int nEntriesRead = 0;
    private long nRepeatIteratorReads = 0;

    /* Per Run counters. Reset before each invocation of the cleaner. */
    private int nINsCleanedThisRun = 0;
    private int nINsMigratedThisRun = 0;
    private int nLNsCleanedThisRun = 0;
    private int nLNsDeadThisRun = 0;
    private int nLNsLockedThisRun = 0;
    private int nLNsMigratedThisRun = 0;
    private int nDeltasCleanedThisRun = 0;

    private int nEntriesReadThisRun;
    private long nRepeatIteratorReadsThisRun;

    private boolean expunge = false;
    private long maxDiskSpace;
    private long diskSpaceTolerance;
    private int minFilesToDelete;

    private FileSelector fileSelector;
    private UtilizationProfile profile;
    private Level detailedTraceLevel;  // level value for detailed trace msgs

    public Cleaner(EnvironmentImpl env, long waitTime, String name,
                   UtilizationProfile profile)
        throws DatabaseException {

        super(waitTime, name, env);
        this.env = env;
        this.profile = profile;

        maxDiskSpace = env.getConfigManager().getLong
            (EnvironmentParams.MAX_DISK_SPACE);
        lockTimeout = PropUtil.microsToMillis(env.getConfigManager().getLong
            (EnvironmentParams.CLEANER_LOCK_TIMEOUT));
	expunge = env.getConfigManager().getBoolean
	    (EnvironmentParams.CLEANER_REMOVE);
        int diskTolerancePct = env.getConfigManager().getInt
            (EnvironmentParams.CLEANER_DISK_SPACE_TOLERANCE);
        diskSpaceTolerance = (maxDiskSpace * diskTolerancePct) / 100;
        minFilesToDelete = env.getConfigManager().getInt
            (EnvironmentParams.CLEANER_MIN_FILES_TO_DELETE);

        if (true) {
            fileSelector = new UtilizationSelector(env, profile);
        } else {
            fileSelector = new RotationSelector(env);
        }

        filesToDelete = new HashSet();

        detailedTraceLevel =
            Tracer.parseLevel(env,
                              EnvironmentParams.JE_LOGGING_LEVEL_CLEANER);
    }

    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("<Cleaner name=\"").append(name).append("\"/>");
        return sb.toString();
    }

    /**
     * Cleaner doesn't have a work queue so just throw an exception if it's
     * ever called.
     */
    public void addToQueue(Object o)
        throws DatabaseException {

        throw new DatabaseException
            ("Cleaner.addToQueue should never be called.");
    }

    /**
     * Load stats.
     */
    public void loadStats(StatsConfig config, EnvironmentStatsInternal stat) 
        throws DatabaseException {

        stat.setNCleanerRuns(nCleanerRuns);
        stat.setNCleanerDeletions(nCleanerDeletions);
        stat.setNINsCleaned(nINsCleaned);
        stat.setNINsMigrated(nINsMigrated);
        stat.setNLNsCleaned(nLNsCleaned);
        stat.setNLNsDead(nLNsDead);
        stat.setNLNsLocked(nLNsLocked);
        stat.setNLNsMigrated(nLNsMigrated);
        stat.setNDeltasCleaned(nDeltasCleaned);
        stat.setNCleanerEntriesRead(nEntriesRead);
        stat.setNRepeatIteratorReads(nRepeatIteratorReads);
        
        if (config.getClear()) {
            nCleanerRuns = 0;
            nCleanerDeletions = 0;
            nINsCleaned = 0;
            nINsMigrated = 0;
            nLNsCleaned = 0;
            nLNsDead = 0;
            nLNsLocked = 0;
            nLNsMigrated = 0;
            nDeltasCleaned = 0;
            nEntriesRead = 0;
            nRepeatIteratorReads = 0;
        }
    }

    public synchronized void clearEnv() {
        env = null;
    }

    /**
     * Return the number of retries when a deadlock exception occurs.
     */
    protected int nDeadlockRetries()
        throws DatabaseException {

        return env.getConfigManager().getInt
            (EnvironmentParams.CLEANER_DEADLOCK_RETRY);
    }

    /**
     * Placeholder for the day when we can detect that an application activity
     * threshold was exceeded.
     */
    private boolean interruptCleaning() {
        return false;
    }

    /**
     * Called whenever the daemon thread wakes up from a sleep.
     */
    public void onWakeup()
        throws DatabaseException {

        /* Clean multiple files, do not force aggressive mode. */
        doClean(true, true, false);
    }

    /**
     * Cleans selected files and returns the number of files cleaned.  May be
     * called by the daemon thread or programatically.
     *
     * @param invokedFromDaemon currently has no effect.
     *
     * @param cleanMultipleFiles is true to clean until we're under * budget,
     * @or false to clean at most one file.
     *
     * @param forceAggressive is true to clean as if we were under budget even
     * when we're not.
     *
     * @return the number of files deleted, not including files cleaned
     * unsuccessfully.
     */
    public synchronized int doClean(boolean invokedFromDaemon,
                                    boolean cleanMultipleFiles,
                                    boolean forceAggressive) 
        throws DatabaseException {

        if (env.isClosed()) {
            return 0;
        }

        int nOriginalLogFiles = profile.getNumberOfFiles();
        boolean anyProgress = false;

        /* Clean until no more files are selected.  */
        int nFilesCleaned = 0;
        while (true) {

            /* Don't clean forever. */
            if (nFilesCleaned >= nOriginalLogFiles) {
                break;
            }

            /* Stop if the daemon is shut down. */
            if (isShutdownRequested()) {
                break;
            }

            /* Select a file for cleaning.  */
            FileRetryInfo fileRetryInfo;
            synchronized (filesToDelete) {
                fileRetryInfo =
                    fileSelector.getFileToClean(filesToDelete,
                                                forceAggressive ||
                                                isNearDiskBudget(true));
            }
            if (fileRetryInfo == null) {
                break;
            }

            /*
             * If we can't get an exclusive lock, then there are reader
             * processes and we can't clean.
             */
            if (!env.getFileManager().lockEnvironment(false, true)) {
                break;
            }

            resetPerRunCounters();

            boolean finished = false;
            Long fileNum = null;
            long fileNumValue = 0;
            boolean canDeleteFile = false;
            try {
                nCleanerRuns++;
                assert Latch.countLatchesHeld() == 0;
                fileNum = fileRetryInfo.getFileNumber();
                fileNumValue = fileNum.longValue();

                Tracer.trace(Level.INFO, env,
                             "CleanerRun " + nCleanerRuns +
                             " on file " +
			     ("0x" + Long.toHexString(fileNumValue)) + 
                             " begins");

                DbLsn[] pendingLsns = fileRetryInfo.getPendingLsns();
                if (pendingLsns != null) {
                    if (processPending(pendingLsns, fileRetryInfo)) {
                        anyProgress = true;
                    }
                }
                if (!fileRetryInfo.isFileFullyProcessed()) {
                    DbConfigManager cm = env.getConfigManager();
                    int readBufferSize =
                        cm.getInt(EnvironmentParams.LOG_ITERATOR_READ_SIZE);
                    DbLsn startLsn = fileRetryInfo.getFirstUnprocessedLsn();
                    CleanerFileReader cleanerFileReader =
                        new CleanerFileReader(env, readBufferSize, startLsn,
                                              fileNum);
                    if (processFile(cleanerFileReader, fileRetryInfo)) {
                        anyProgress = true;
                    }
                    nRepeatIteratorReadsThisRun =
                        cleanerFileReader.getNRepeatIteratorReads();
                }

                if (fileRetryInfo.canFileBeDeleted()) {
                    synchronized (filesToDelete) {
                        filesToDelete.add(fileNum);
                    }
                    canDeleteFile = true;
                    nFilesCleaned += 1;
                }
                accumulatePerRunCounters();
                finished = true;
                if (!cleanMultipleFiles) {
                    break;
                }
            } catch (IOException IOE) {
                Tracer.trace(env, "Cleaner", "doClean", "", IOE);
                throw new DatabaseException(IOE);
            } finally {
                fileRetryInfo.endProcessing(canDeleteFile);
                env.getFileManager().releaseExclusiveLock();
                Tracer.trace(Level.SEVERE, env,
                             "CleanerRun " + nCleanerRuns + 
                             " on file " + 
                             (fileNum == null ? "none" :
                              ("0x" + Long.toHexString(fileNum.longValue()))) + 
                             " invokedFromDaemon=" + invokedFromDaemon +
                             " finished=" + finished +
                             " canDelete=" + canDeleteFile +
                             " nEntriesRead=" + nEntriesReadThisRun +
                             " nINsCleaned=" + nINsCleanedThisRun +
                             " nINsMigrated=" + nINsMigratedThisRun +
                             " nLNsCleaned=" + nLNsCleanedThisRun +
                             " nLNsDead=" + nLNsDeadThisRun +
                             " nLNsMigrated=" + nLNsMigratedThisRun +
                             " nLNsLocked=" + nLNsLockedThisRun +
                             " nDeltasCleaned= " + nDeltasCleanedThisRun);
            }
        }

        return nFilesCleaned;
    }

    /**
     * Called when starting a checkpoint to get the files to be deleted when
     * the checkpoint is complete.
     *
     * <p>If non-null is returned, the checkpoint should not allow deltas, the
     * checkpoint should flush to the root, and deleteCleanedFiles() should be
     * called when the checkpoint is complete.  The caller must call
     * deleteCleanedFiles() or the files will have to be cleaned again
     * later.</p>
     *
     * <p>The checkpoint can't have deltas because the checkpointer is
     * essentially rewriting INs for the cleaner. We flush all the way to the
     * root to make sure there are no references left to the old file.</p>
     *
     * @param getAll is true to get all files even if the number of cleaned
     * files is below the threshold.  Even if getAll is true, null will be
     * returned if zero files have been cleaned.
     */
    public Set getCleanedFiles(boolean getAll) {
        synchronized (filesToDelete) {
            int minFiles = getAll ? 1 : minFilesToDelete;
            if (filesToDelete.size() < minFiles) {
                return null;
            } else {
                return new HashSet(filesToDelete);
            }
        }
    }

    /**
     * Delete files that were previously returned by getCleanedFiles, after
     * performing a checkpoint with no deltas and that flushed to the root.
     */
    public void deleteCleanedFiles(Set cleanedFiles)
        throws DatabaseException {

        int nFilesDeleted = 0;
        try {
            for (Iterator i = cleanedFiles.iterator(); i.hasNext();) {
                Long fileNum = (Long) i.next();
                long fileNumValue = fileNum.longValue();
                synchronized (filesToDelete) {
                    assert filesToDelete.contains(fileNum);
                }
                try {
                    if (expunge) {
                        env.getFileManager().deleteFile(fileNumValue);
                    } else {
                        env.getFileManager().renameFile
                            (fileNumValue, FileManager.DEL_SUFFIX);
                    }
                } catch (IOException e) {
                    Tracer.trace(env, "Cleaner", "deleteCleanedFiles", "", e);
                    throw new DatabaseException(e);
                }
                profile.removeFile(fileNum);
                synchronized (filesToDelete) {
                    filesToDelete.remove(fileNum);
                }
                nFilesDeleted++;
                nCleanerDeletions++;
            }
        } finally {
            Tracer.trace(Level.SEVERE, env,
                         "CleanerRun checkpoint complete" +
                         " nFilesDeleted=" + nFilesDeleted);
        }
    }

    /**
     * Returns whether we are near to using the disk budget.
     *
     * @param calcIfNecessary is true if this method should calculate the total
     * log size by reading the utilization database if the size is not already
     * available.  It will only calculate it the first time it is called and if
     * the cleaner has not yet been invoked.
     *
     * @return true if we are near the disk budget or, to be conservative, if
     * the total log size is not available and calcIfNecessary is false.  False
     * is returned only if we have calculated the total size and we are not
     * near the budget.
     */
    public boolean isNearDiskBudget(boolean calcIfNecessary)
        throws DatabaseException {
        
        long size = profile.getTotalLogSize(calcIfNecessary);
        return (size < 0) ||
               (maxDiskSpace < size) ||
               (maxDiskSpace - size <= diskSpaceTolerance);
    }

    /**
     * Process all pending LNs in the "processed" portion of the file.  These
     * were previously processed by processFile() which set their disposition
     * to pending because processLN() returned false.  They are all LNs.
     */
    private boolean processPending(DbLsn[] pendingLsns,
                                   FileRetryInfo fileRetryInfo)
        throws DatabaseException, IOException {

        DbTree dbMapTree = env.getDbMapTree();
        TreeLocation location = new TreeLocation();
        LogManager logManager = env.getLogManager();
        boolean anyProgress = false;

        for (int i = 0; i < pendingLsns.length; i += 1) {
            DbLsn lsn = pendingLsns[i];
            if (interruptCleaning()) {
                break;
            }
            LogEntry entry = logManager.getLogEntry(lsn);
            nEntriesRead += 1;
            if (entry instanceof LNLogEntry) {
                LNLogEntry lnEntry = (LNLogEntry) entry;
                LN targetLN = lnEntry.getLN();
                long nodeId = targetLN.getNodeId();
                DatabaseId dbId = lnEntry.getDbId();
                Key key = lnEntry.getKey();
                Key dupKey = lnEntry.getDupKey();
                DatabaseImpl db = dbMapTree.getDb(dbId, lockTimeout);
                if (processLN(targetLN, db, key, dupKey, lsn, location)) {
                    fileRetryInfo.setObsoleteLN(lsn, nodeId);
                    anyProgress = true;
                }
            } else {
                assert false: entry.getClass().getName();
            }
        }
        return anyProgress;
    }

    /**
     * Process all LSNs from the position of the given reader onward.  This is
     * the "unprocessed" portion of the log file.  The FileSelector may have
     * knowledge that LNs in this portion of the file are obsolete, but does
     * not have information on INs.  When we process an LN, we inform the
     * FileSelector of the node disposition.  The FileSelector assumes that we
     * process all INs successfully, so we don't need to inform it about them.
     * If we stop processing before reaching the end of the file, we tell the
     * FileSelector what LSN is next.
     */
    private boolean processFile(CleanerFileReader reader,
                                FileRetryInfo fileRetryInfo)
        throws DatabaseException, IOException {

        DbTree dbMapTree = env.getDbMapTree();
        TreeLocation location = new TreeLocation();
        boolean anyProgress = false;
        boolean interrupted = false;

        while (reader.readNextEntry()) {
            nEntriesRead += 1;
            DbLsn lsn = reader.getLastLsn();
            fileRetryInfo.setFirstUnprocessedLsn(lsn);
            if (interruptCleaning()) {
                interrupted = true;
                break;
            }
            if (reader.isLN()) {
                LN targetLN = reader.getLN();
                long nodeId = targetLN.getNodeId();
                if (!fileRetryInfo.isObsoleteLN(lsn, nodeId)) {
                    DatabaseId dbId = reader.getDatabaseId();
                    Key key = reader.getKey();
                    Key dupKey = reader.getDupTreeKey();
                    DatabaseImpl db = dbMapTree.getDb(dbId, lockTimeout);
                    if (processLN(targetLN, db, key, dupKey, lsn, location)) {
                        fileRetryInfo.setObsoleteLN(lsn, nodeId);
                        anyProgress = true;
                    } else {
                        fileRetryInfo.setPendingLN(lsn, nodeId);
                    }
                }
            } else if (reader.isIN()) {
                IN targetIN = reader.getIN();
                DatabaseId dbId = reader.getDatabaseId();
                DatabaseImpl db = dbMapTree.getDb(dbId, lockTimeout);
                targetIN.setDatabase(db);
                processIN(targetIN, db, lsn);
                anyProgress = true;
            } else if (reader.isRoot()) {
                env.rewriteMapTreeRoot(lsn);
                anyProgress = true;
            } else if (reader.isDelta()) {
                DatabaseId dbId = reader.getDatabaseId();
                DatabaseImpl db = dbMapTree.getDb(dbId, lockTimeout);
                processDelta(reader, db, lsn);
                anyProgress = true;
            }
        }
        if (!interrupted) {
            fileRetryInfo.setFileFullyProcessed();
        }
        nEntriesReadThisRun = reader.getNumRead();
        return anyProgress;
    }

    /**
     * Return true if the LN was processed successfully (either it was migrated
     * or it is no longer active).  Return false if this LN couldn't be
     * migrated, but is still active.
     */
    private boolean processLN(LN lnClone, DatabaseImpl db, Key key,
                              Key dupKey, DbLsn logLsn, TreeLocation location)
        throws DatabaseException {

        /* Status variables are used to generate debug tracing info. */
        boolean obsolete = false;  // if true, LN is no longer in use.
        boolean migrated = false;  // if true, LN was in use and is migrated.
        boolean completed = false; // if true, this method completed.

        BasicLocker locker = null;
        DIN parentDIN = null;      // for DupCountLNS
        boolean insPinned = false;
        try {

            /* The whole database is gone, so this LN is obsolete. */
            if (db == null || db.getIsDeleted()) {
                obsolete = true;
                completed = true;
                return true;
            }

            boolean cleaned = true;
            nLNsCleanedThisRun++;

            Tree tree = db.getTree();
            assert tree != null;

            /*
	     * Search down to the bottom most level for the parent of this LN.
	     */
            boolean found =
		tree.getParentBINForChildLN(location, key, dupKey, lnClone,
					    false, true, false);

            boolean lnIsDupCountLN = lnClone.containsDuplicates();
            if (!found) {
                obsolete = true;
                nLNsDeadThisRun++;
		completed = true;
		return cleaned;
            }

	    /*
	     * Now we're at the parent for this LN, whether BIN, DBIN or
	     * DIN. Get the child reference.
	     */
	    ChildReference ref = location.bin.getEntry(location.index);

	    /*
	     * If knownDeleted, LN is deleted and can be purged.
	     */
	    if (ref.isKnownDeleted()) {
		nLNsDeadThisRun++;
		obsolete = true;
		completed = true;
		return cleaned;
	    }

	    IN lnParent = null;
	    if (lnIsDupCountLN) {
		parentDIN = (DIN) ref.fetchTarget(db, location.bin);
		parentDIN.latch();
		ref = parentDIN.getDupCountLNRef();
		lnParent = parentDIN;
	    } else {
		lnParent = location.bin;
	    }

	    /*
	     * LN is current. Now we have to see if it's in an active
	     * locker. Do that by attempting to lock it. Note that we'll have
	     * to release the appropriate latches before trying to lock, so be
	     * sure to pin the INs so it doesn't get evicted during the
	     * unlatched period.
	     */
	    LN lnInTree = (LN) ref.fetchTarget(db, lnParent);
	    insPinned = true;
	    if (lnIsDupCountLN) {
		parentDIN.setEvictionProhibited(true);
		parentDIN.releaseLatch();
	    }
	    location.bin.setEvictionProhibited(true);
	    location.bin.releaseLatch();

	    locker = new BasicLocker(env);
	    locker.setLockTimeout(lockTimeout);
	    LockGrantType lock = locker.nonBlockingReadLock(lnInTree);
	    if (lock == LockGrantType.DENIED) {

		/* 
		 * LN is currently locked by another Locker, so we can't assume
		 * anything about the value of the lsn in the bin.
		 */
		DbLsn abortLsn = locker.getOwnerAbortLsn(lnInTree.getNodeId());
		if ((abortLsn != null) &&
		    abortLsn != DbLsn.NULL_LSN &&
		    abortLsn.compareTo(logLsn) > 0) {
		    nLNsDeadThisRun++;
		    obsolete = true;
		} else {
		    nLNsLockedThisRun++;
		    cleaned = false;
		}

		completed = true;
		return cleaned;
	    }

	    /*
	     * We were able to lock this LN in the tree. Now regain the BIN
	     * latch and try to migrate this LN to the end of the log file so
	     * we can throw away the old log entry.  Only do any of this if the
	     * BIN & LN didn't change out from underneath us while we were
	     * trying the lock attempt.
	     */
	    location.bin.latch();
	    location.bin.setEvictionProhibited(false);
	    ChildReference newRef = location.bin.getEntry(location.index);
	    if (lnIsDupCountLN) {
		Node n = newRef.fetchTarget(db, location.bin);
		if (n instanceof DIN) {
		    parentDIN = (DIN) n;
		    parentDIN.latch();
		    newRef = parentDIN.getDupCountLNRef();
		    parentDIN.setEvictionProhibited(false);
		}
	    }
	    insPinned = false;

            if (newRef.getTarget() == lnInTree) {

                /* 
                 * This is still the same lnInTree, nothing changed. We know
                 * it's not in an active locker because we were able to lock
                 * it.
                 */
                if (lnInTree.isDeleted()) {

                    /*
                     * If the LN is deleted, we must set knownDeleted to
                     * prevent fetching it later.  This could occur when
                     * scanning over deleted entries that have not been
                     * compressed away [10553].
                     */
                    assert !lnIsDupCountLN;
                    location.bin.setKnownDeletedLeaveTarget(location.index);
                    nLNsDeadThisRun++;
                    obsolete = true;
                } else {

                    /* 
                     * 1. If the LSN in the tree and in the log are the same,
                     * we can migrate.
                     * 
                     * 2. If the LSN in the tree is < the LSN in the log, the
                     * log entry is obsolete, because this LN has been rolled
                     * back to a previous version by a txn that aborted.
                     * 
                     * 3. If the LSN in the tree is > the LSN in the log, the
                     * log entry is obsolete, because the LN was advanced
                     * forward by some now-committed txn.
                     */
                    if (newRef.getLsn().equals(logLsn)) {
                        DbLsn newLNLsn =
                            migrateLN(lnInTree, db,
                                      key, logLsn, locker);
                        if (lnIsDupCountLN) {
                            parentDIN.updateDupCountLNRef(newLNLsn);
                        } else {
                            location.bin.updateEntry(location.index,
                                                     newLNLsn);
                        }
                        migrated = true;
                    } else {
                        /* LN is obsolete and can be purged. */
                        nLNsDeadThisRun++;
                        obsolete = true;
                    }
                }
	    } else {
		cleaned = false;
	    }
            completed = true;
            return cleaned;
	} catch (DatabaseException DBE) {
	    DBE.printStackTrace();
	    Tracer.trace(env, "com.sleepycat.je.cleaner.Cleaner", "processLN",
			 "Exception thrown: ", DBE);
	    throw DBE;
        } finally {
            unPinAndRelease(parentDIN, insPinned);
            unPinAndRelease(location.bin, insPinned);

            if (locker != null) {
                locker.operationEnd();
            }

            trace(detailedTraceLevel, CLEAN_LN, lnClone, logLsn,
                  completed, obsolete, migrated);
        }
    }

    private void unPinAndRelease(IN in, boolean isPinned)
        throws DatabaseException {
        	
        if (in != null) {
            if (isPinned) {
                if (!in.getLatch().isOwner()) {
                    in.latch();
                }
                in.setEvictionProhibited(false);
                in.releaseLatch();
            } else {
                if (in.getLatch().isOwner()) {
                    in.releaseLatch();
                }
            }
        }
    }

    private DbLsn migrateLN(LN ln, DatabaseImpl db, Key key, DbLsn oldLsn,
                            Locker locker)
        throws DatabaseException {

        DbLsn newLsn = ln.log(env, db.getId(), key, oldLsn, locker);
        nLNsMigratedThisRun++;
        return newLsn;
    }

    /**
     * If an IN is still in use in the in-memory tree, dirty it. The checkpoint
     * invoked at the end of the cleaning run will end up rewriting it.
     */
    private void processIN(IN inClone, DatabaseImpl db, DbLsn lsn)
        throws DatabaseException {

        boolean obsolete = false;
        boolean dirtied = false;
        boolean completed = false;

        try {
            if (db == null || db.getIsDeleted()) {
                obsolete = true;
                completed = true;
                return;
            }

            Tree tree = db.getTree();
            assert tree != null;
            IN inInTree = findINInTree(tree, db, inClone, lsn);

            if (inInTree == null) {
                /* IN is no longer in the tree.  Do nothing. */
                nINsCleanedThisRun++;
                obsolete = true;
            } else {
                /* 
                 * IN is still in the tree.  Dirty it.  Checkpoint will write
                 * it out.
                 */
                nINsMigratedThisRun++;
                inInTree.setDirty(true);
                inInTree.setCleanedSinceLastLog();
                inInTree.releaseLatch();
                dirtied = true;
            }

            completed = true;
        } finally {
            trace(detailedTraceLevel, CLEAN_IN, inClone, lsn,
                  completed, obsolete, dirtied);
        }
    }

    /**
     * Given a clone of an IN that has been taken out of the log, try to find
     * it in the tree and verify that it is the current one in the log.
     * Returns the node in the tree if it is found and it is current re: lsn's.
     * Otherwise returns null if the clone is not found in the tree or it's not
     * the latest version.  Caller is responsible for unlatching the returned
     * IN.
     */
    private IN findINInTree(Tree tree, DatabaseImpl db, IN inClone, DbLsn lsn)
        throws DatabaseException {

        /* Check if inClone is the root. */
        if (inClone.isDbRoot()) {
            IN rootIN = isRoot(tree, db, inClone, lsn);
            if (rootIN == null) {

                /*
                 * inClone is a root, but no longer in use. Return now, because
                 * a call to tree.getParentNode will return something
                 * unexpected since it will try to find a parent.
                 */
                return null;  
            } else {
                return rootIN;
            }
        }       

        /* It's not the root.  Can we find it, and if so, is it current? */
        inClone.latch();
        SearchResult result = null;
        try {
            result = tree.getParentINForChildIN(inClone, true);
            if (!result.exactParentFound) {
                return null;
            }
        
            ChildReference ref = result.parent.getEntry(result.index);
            int compareVal = ref.getLsn().compareTo(lsn);
            
            if (compareVal > 0) {
                /* Log entry is obsolete. */
                return null;
            } else {
                /*
                 * Log entry is same or newer than what's in the tree.  Dirty
                 * the IN and let checkpoint write it out.
                 */
                IN in = (IN) ref.fetchTarget(db, result.parent);
                in.latch();
                return in;
            }
        } finally {
            if ((result != null) && (result.exactParentFound)) {
                result.parent.releaseLatch();
            }
        }
    }

    /**
     * See if a BINDelta is in use in the tree. If it is, dirty its owning node
     * and the checkpoint at the end of cleaning will write out a full version.
     */
    private void processDelta(CleanerFileReader cleanerReader,
                              DatabaseImpl db, DbLsn lsn)
        throws DatabaseException {

        boolean obsolete = false;
        boolean dirtied = false;
        boolean completed = false;

        IN owningBIN = null;
        try {
            if (db == null || db.getIsDeleted()) {
                obsolete = true;
                completed = true;
                nDeltasCleanedThisRun++;
                return;
            } 

            IN reconstructed = null;
            try {
                reconstructed = cleanerReader.getReconstructedIN();
            } catch (LogFileNotFoundException e) {
                /* 
                 * Do nothing, the BIN that this delta belongs to is 
                 * obsolete
                 */
                obsolete = true;
                completed = true;
                nDeltasCleanedThisRun++;
                return;
            }

            if (reconstructed == null) {
                obsolete = true;
            } else {
                Tree tree = db.getTree();
                assert tree != null;

                owningBIN = findDeltaInTree(tree, db, reconstructed, lsn);
                if (owningBIN == null) {
                    /* This delta is no longer needed. */
                    obsolete = true;
                } else {
                    /* 
                     * The delta is still in use. Get rid of it by forcing a
                     * write of the delta owner.
                     */
                    nINsMigratedThisRun++;

                    owningBIN.setDirty(true);
                    owningBIN.releaseLatch();
                    dirtied = true;
                }

            }
            nDeltasCleanedThisRun++;
            completed = true;
        } finally {
            trace(detailedTraceLevel, CLEAN_DELTA, owningBIN,
                  lsn, completed, obsolete, dirtied);
        }
    }

    /**
     * Given a reconstructed BIN and the lsn of the delta, find out if the
     * delta is in use. If it is, return the owning BIN, else return null.
     */
    private IN findDeltaInTree(Tree tree, DatabaseImpl db,
                               IN reconstructed, DbLsn lsn)
        throws DatabaseException {

        reconstructed.latch();
        SearchResult result = tree.getParentINForChildIN(reconstructed, true);
        if (!result.exactParentFound) {
            return null;
        }

        BIN binInTree =
            (BIN) result.parent.getEntry(result.index).fetchTarget(db,
                                                               result.parent);
        binInTree.latch();
        result.parent.releaseLatch();

        DbLsn lastDeltaLsn = binInTree.getLastDeltaVersion();

        if ((lastDeltaLsn != null) &&
            (lastDeltaLsn.compareTo(lsn) <= 0)) {
            return binInTree;
        } else {
            binInTree.releaseLatch();
            return null;
        } 
    }

    private static class RootDoWork implements WithRootLatched {
        private DatabaseImpl db;
        private IN inClone;
        private DbLsn lsn;

        RootDoWork(DatabaseImpl db, IN inClone, DbLsn lsn) {
            this.db = db;
            this.inClone = inClone;
            this.lsn = lsn;
        }

        public IN doWork(ChildReference root)
            throws DatabaseException {

            if (root == null ||
		((IN) root.fetchTarget(db, null)).getNodeId() !=
                inClone.getNodeId()) {
                return null;
            }

            if (root.getLsn().compareTo(lsn) <= 0) {
                IN rootIN = (IN) root.fetchTarget(db, null);
                rootIN.latch();
                return rootIN;
            } else {
                return null;
            }
        }
    }

    /**
     * Check if the cloned IN is the same node as the root in tree.  Return the
     * real root if it is, null otherwise.  If non-null is returned, the
     * returned IN (the root) is latched -- caller is responsible for
     * unlatching it.
     */
    private IN isRoot(Tree tree, DatabaseImpl db, IN inClone, DbLsn lsn)
        throws DatabaseException {

        RootDoWork rdw = new RootDoWork(db, inClone, lsn);
        return tree.withRootLatched(rdw);
    }

    /**
     * Reset per-run counters.
     */
    private void resetPerRunCounters() {
        nINsCleanedThisRun = 0;
        nINsMigratedThisRun = 0;
        nLNsCleanedThisRun = 0;
        nLNsDeadThisRun = 0;
        nLNsMigratedThisRun = 0;
        nLNsLockedThisRun = 0;
        nDeltasCleanedThisRun = 0;
        nEntriesReadThisRun = 0;
        nRepeatIteratorReadsThisRun = 0;
    }

    private void accumulatePerRunCounters() {
        nINsCleaned +=     nINsCleanedThisRun;
        nINsMigrated +=    nINsMigratedThisRun;
        nLNsCleaned +=     nLNsCleanedThisRun;
        nLNsDead +=        nLNsDeadThisRun;
        nLNsMigrated +=    nLNsMigratedThisRun;
        nLNsLocked +=      nLNsLockedThisRun;
        nDeltasCleaned +=  nDeltasCleanedThisRun;
        nRepeatIteratorReads += nRepeatIteratorReadsThisRun;
    }

    /**
     * Send trace messages to the java.util.logger. Don't rely on the logger
     * alone to conditionalize whether we send this message, we don't even want
     * to construct the message if the level is not enabled.
     */
    private void trace(Level level,
                       String action,
                       Node node,
                       DbLsn logLsn,
                       boolean completed,
                       boolean obsolete,
                       boolean dirtiedMigrated) {

        Logger logger = env.getLogger();
        if (logger.isLoggable(level)) {
            StringBuffer sb = new StringBuffer();
            sb.append(action);
            if (node != null) {
                sb.append(" node=");
                sb.append(node.getNodeId());
            }
            sb.append(" logLsn=");
            sb.append(logLsn.getNoFormatString());
            sb.append(" complete=").append(completed);
            sb.append(" obsolete=").append(obsolete);
            sb.append(" dirtiedOrMigrated=").append(dirtiedMigrated);

            logger.log(level, sb.toString());
        }
    }
}
