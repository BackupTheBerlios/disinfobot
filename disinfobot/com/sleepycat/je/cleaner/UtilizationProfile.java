/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: UtilizationProfile.java,v 1.1 2004/11/22 18:27:58 kate Exp $
*/

package com.sleepycat.je.cleaner;

import java.util.Iterator;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.CursorImpl;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbTree;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.CursorImpl.SearchMode;
import com.sleepycat.je.log.FileManager;
import com.sleepycat.je.tree.FileSummaryLN;
import com.sleepycat.je.tree.Key;
import com.sleepycat.je.txn.AutoTxn;
import com.sleepycat.je.txn.BasicLocker;
import com.sleepycat.je.txn.Locker;
import com.sleepycat.je.utilint.DbLsn;

/**
 * The UP tracks utilization summary information for all log files.
 *
 * <p>Unlike the UtilizationTracker, the UP is not accessed under the log write
 * latch and is instead synchronized on itself.  It is only accessed by three
 * entities:  the cleaner, the checkpointer, and the recovery manager.  It is
 * not accessed during the primary data access path.</p>
 *
 * <p>The recovery manager calls cacheFileSummary when it reads a file summary
 * LN.  The cleaner will ask the UP to populate its cache in order to determine
 * the total log size or to select the best file for cleaning.  The UP will
 * then read all records in the UP database that are not already cached.  The
 * checkpointer calls putFileSummary to write file summary LNs to the log.</p>
 *
 * <p>Because this object is synchronized it is possible that the cleaner will
 * hold up the checkpointer if the cleaner is populating its cache or
 * calculating the best file, and the checkpointer tries to write the file
 * summary LNs to the log.  This blocking is acceptable.  Deadlocks will not
 * occur since calls are always from the checkpointer or cleaner to the UP, and
 * not in the other direction.</p>
 */
public class UtilizationProfile {

    /*
     * Note that age is a distance between files not a number of files, that
     * is, deleted files are counted in the age.
     */
    private EnvironmentImpl env;
    private UtilizationTracker tracker;
    private DatabaseImpl fileSummaryDb; // stored fileNum -> FileSummary
    private SortedMap fileSummaryMap;   // cached fileNum -> FileSummary
    private boolean cachePopulated;
    private long totalLogSize;

    /* Minimum utilization threshold that triggers cleaning. */
    private int minUtilization;

    /*
     * Minumum age to qualify for cleaning.  If the first active LSN file is 5
     * and the mininum age is 2, file 4 won't qualify but file 3 will.  Must be
     * greater than zero because we never clean the first active LSN file.
     */
    private int minAge;

    private int obsoleteAge;

    /**
     * Creates an empty UP.
     */
    public UtilizationProfile(EnvironmentImpl env,
                              UtilizationTracker tracker)
        throws DatabaseException {

        this.env = env;
        this.tracker = tracker;
        fileSummaryMap = new TreeMap();

        minAge = env.getConfigManager().getInt
            (EnvironmentParams.CLEANER_MIN_AGE);
        minUtilization = env.getConfigManager().getInt
            (EnvironmentParams.CLEANER_MIN_UTILIZATION);
        obsoleteAge = env.getConfigManager().getInt
            (EnvironmentParams.CLEANER_OBSOLETE_AGE);
    }

    /**
     * Returns the IN obsolete age config setting.
     */
    final int getObsoleteAge() {
        return obsoleteAge;
    }

    /**
     * Returns the number of files in the profile.
     */
    synchronized int getNumberOfFiles()
        throws DatabaseException {

        populateCache();
        return fileSummaryMap.size();
    }

    /**
     * Returns the best file that qualifies for cleaning, or null if no file
     * qualifies.  This method is not thread safe and should only be called
     * from the cleaner thread.
     */
    synchronized Long getBestFileForCleaning(Set excludeFiles,
                                             boolean aggressive)
        throws DatabaseException {

        /* Populate the cache. */
        populateCache();

        /* Paranoia.  There should always be 1 file. */
        if (fileSummaryMap.size() == 0) {
            return null;
        }

        /* There must have been at least one checkpoint previously. */
        DbLsn firstActiveLsn = env.getCheckpointer().getFirstActiveLsn();
        if (firstActiveLsn == null) {
            return null;
        }

        /* Calculate totals and find the best file. */
        Iterator iter = fileSummaryMap.keySet().iterator();
        int fileIndex = 0;
        Long bestFile = null;
        int bestUtilization = 101;
        long totalSize = 0;
        long totalObsoleteSize = 0;

        while (iter.hasNext()) {
            Long file = (Long) iter.next();
            long fileNum = file.longValue();

            /* Don't count or select files that are too young or excluded. */
            if (firstActiveLsn.getFileNumber() - fileNum < minAge ||
                excludeFiles.contains(file)) {
                continue;
            }

            /* Calculate this file's value and add it to the totals. */
            FileSummary summary = (FileSummary) fileSummaryMap.get(file);

            summary = addTrackedSummary(summary, fileNum);
            int obsoleteSize = summary.getObsoleteSize(fileIndex, this);
            totalObsoleteSize += obsoleteSize;
            totalSize += summary.totalSize;

            /* Select this file if it has the least utilization so far. */
            int thisUtilization = utilization(obsoleteSize, summary.totalSize);
            if (bestFile == null || thisUtilization < bestUtilization) {
                bestFile = file;
                bestUtilization = thisUtilization;
            }

            fileIndex += 1;
        }

        /*
         * Return the best file if we are under the minimum utilization or
         * we're cleaning aggressively.
         */
        int totalUtilization = utilization(totalObsoleteSize, totalSize);
        if (aggressive || totalUtilization < minUtilization) {
            return bestFile;
        } else {
            return null;
        }
    }

    /**
     * Calculate the utilization percentage.
     */
    public static int utilization(long obsoleteSize, long totalSize) {
        if (totalSize != 0) {
            return (int) (((totalSize - obsoleteSize) * 100) / totalSize);
        } else {
            return 0;
        }
    }

    /**
     * Add the tracked summary, if one exists, to the base summary.
     */
    private FileSummary addTrackedSummary(FileSummary summary, long fileNum) {

        TrackedFileSummary trackedSummary = tracker.getTrackedFile(fileNum);
        if (trackedSummary != null) {
            FileSummary totals = new FileSummary();
            totals.add(summary);
            totals.add(trackedSummary);
            summary = totals;
        }
        return summary;
    }

    /**
     * Returns the total byte size of all log files in the environment, or
     * -1 if the size is not available and calcIfNecessary is false.
     *
     * FindBugs will complain about this not being synchronized, but it's ok.
     */
    public long getTotalLogSize(boolean calcIfNecessary)
        throws DatabaseException {

        if (calcIfNecessary) {
            populateCache();
        }
        if (cachePopulated) {
            return totalLogSize + tracker.getLogSizeDelta();
        } else {
            return -1;
        }
    }

    /**
     * Returns a copy of the current file summary map, optionally including
     * tracked summary information, for use by the DbSpace utility and by unit
     * tests.  The returned map's key is a Long file number and its value is a
     * FileSummary.
     */
    public synchronized SortedMap getFileSummaryMap(boolean includeTrackedFiles)
        throws DatabaseException {

        populateCache();
        if (includeTrackedFiles) {
            TreeMap map = new TreeMap();
            Iterator iter = fileSummaryMap.keySet().iterator();
            while (iter.hasNext()) {
                Long file = (Long) iter.next();
                long fileNum = file.longValue();
                FileSummary summary = (FileSummary) fileSummaryMap.get(file);
                summary = addTrackedSummary(summary, fileNum);
                map.put(file, summary);
            }
            TrackedFileSummary[] trackedFiles = tracker.getTrackedFiles();
            for (int i = 0; i < trackedFiles.length; i += 1) {
                TrackedFileSummary summary = trackedFiles[i];
                long fileNum = summary.getFileNumber();
                Long file = new Long(fileNum);
                if (!map.containsKey(file)) {
                    map.put(file, summary);
                }
            }
            return map;
        } else {
            return new TreeMap(fileSummaryMap);
        }
    }

    /**
     * Clears the cache of file summary info.  The cache starts out unpopulated
     * and is populated on the first call to getBestFileForCleaning.
     */
    public synchronized void clearCache() {
        fileSummaryMap = new TreeMap();
        cachePopulated = false;
        totalLogSize = 0;
    }

    /**
     * Populate the profile for file selection.
     */
    private synchronized void populateCache()
        throws DatabaseException {

        /*
         * It is possible to have an undeleted FileSummaryLN in the database
         * for a deleted log file.  Therefore only read records that have
         * existing (non-deleted) log files.
         */
        if (!cachePopulated) {
            FileManager fileManager = env.getFileManager();
            Long[] nums = fileManager.getAllFileNumbers();
            for (int i = 0; i < nums.length; i += 1) {
                getFileSummary(nums[i]);
            }
            cachePopulated = true;
        }
    }

    /**
     * Caches a given file summary when the summary has been read from the log
     * during recovery or has been written to the log during a checkpoint.
     */
    private synchronized void cacheFileSummary(long fileNum,
                                               FileSummary summary) {

        FileSummary oldSummary = (FileSummary)
            fileSummaryMap.put(new Long(fileNum), summary);
        if (oldSummary != null) {
            totalLogSize -= oldSummary.totalSize;
        }
        totalLogSize += summary.totalSize;
    }

    /**
     * Removes a file from the utilization database and the profile, after it
     * has been deleted by the cleaner.
     */
    synchronized void removeFile(Long fileNum)
        throws DatabaseException {

        /* Remove from the cache. */
        FileSummary oldSummary = (FileSummary) fileSummaryMap.remove(fileNum);
        if (oldSummary != null) {
            totalLogSize -= oldSummary.totalSize;
        }

        /*
         * Open the file summary db on first use.  Since this is called from
         * the cleaner, we should never be in a read-only environment.
         */
        boolean opened = openFileSummaryDatabase();
        assert opened;

        /* Delete from the summary db. */
        Locker locker = null;
        CursorImpl cursor = null;
        try {
            locker = new BasicLocker(env);
            cursor = new CursorImpl(fileSummaryDb, locker);
            byte[] keyBytes =
                FileSummaryLN.fileNumberToBytes(fileNum.longValue());
            DatabaseEntry keyEntry = new DatabaseEntry(keyBytes);

            /* Search by file number and delete. */
            int result = cursor.searchAndPosition(keyEntry,
                                                  new DatabaseEntry(),
                                                  SearchMode.SET,
                                                  null);
            if ((result & CursorImpl.FOUND) != 0) {
                cursor.delete();
            }
        } finally {
            if (cursor != null) {
                cursor.releaseBINs();
                cursor.close();
            }
            if (locker != null) {
                locker.operationEnd();
            }
        }
    }

    /**
     * Returns the stored FileSummary for a given file, or null if none exists
     * in the FileSummary database.  The summary returned does not include
     * the delta information managed by the UtilizationTracker.
     */
    private synchronized FileSummary getFileSummary(Long fileNum)
        throws DatabaseException {

        /* Return cached version if available. */
        FileSummary summary = (FileSummary) fileSummaryMap.get(fileNum);
        if (summary != null) {
            return summary;
        }

        /* Open the file summary db on first use. */
        if (!openFileSummaryDatabase()) {
            /* Db does not exist and this is a read-only environment. */
            return null;
        }

        /* Retrieve from the summary db. */
        Locker locker = null;
        CursorImpl cursor = null;
        try {
            locker = new BasicLocker(env);
            cursor = new CursorImpl(fileSummaryDb, locker);
            byte[] keyBytes =
                FileSummaryLN.fileNumberToBytes(fileNum.longValue());
            DatabaseEntry keyEntry = new DatabaseEntry(keyBytes);

            /* Search by file number. */
            int result = cursor.searchAndPosition(keyEntry,
                                                  new DatabaseEntry(),
                                                  SearchMode.SET,
                                                  null);
            if ((result & CursorImpl.FOUND) != 0) {
                FileSummaryLN ln = (FileSummaryLN)
                    cursor.getCurrentLNAlreadyLatched(LockMode.DEFAULT);

                /* Cache and return file summary. */
                summary = ln.getBaseSummary();
                cacheFileSummary(fileNum.longValue(), summary);
                return summary;
            } else {
                return null;
            }
        } finally {
            if (cursor != null) {
                cursor.releaseBINs();
                cursor.close();
            }
            if (locker != null) {
                locker.operationEnd();
            }
        }
    }

    /**
     * Updates and stores the FileSummary for a given tracked file and returns
     * the updated summary.
     */
    public synchronized FileSummary putFileSummary(TrackedFileSummary
                                                   trackedSummary)
        throws DatabaseException {

        if (env.isReadOnly()) {
            throw new DatabaseException
                ("Cannot write file summary in a read-only environment");
        }

        if (trackedSummary.isEmpty()) {
            return null; // no delta
        }

        /* Open the file summary db on first use. */
        boolean opened = openFileSummaryDatabase();
        assert opened;

        /* Get file number. */
        long fileNum = trackedSummary.getFileNumber();
        Long fileNumLong = new Long(fileNum);
        byte[] keyBytes = FileSummaryLN.fileNumberToBytes(fileNum);

        /* Get previously cached total size. */
        int oldTotalSize = 0;
        FileSummary oldSummary = (FileSummary) fileSummaryMap.get(fileNumLong);
        if (oldSummary != null) {
            oldTotalSize = oldSummary.totalSize;
        }

        Locker locker = null;
        CursorImpl cursor = null;
        try {

            /* Retrieve from the summary db. */
            locker = new BasicLocker(env);
            FileSummaryLN ln = null;
            try {
                cursor = new CursorImpl(fileSummaryDb, locker);
                DatabaseEntry keyEntry = new DatabaseEntry(keyBytes);

                /* Search by key. */
                int result = cursor.searchAndPosition(keyEntry,
                                                      new DatabaseEntry(),
                                                      SearchMode.SET,
                                                      null);
                if ((result & CursorImpl.FOUND) != 0) {
                    /* Summary exists, update it. */
                    ln = (FileSummaryLN)
                        cursor.getCurrentLNAlreadyLatched(LockMode.DEFAULT);
                    ln.setTrackedSummary(trackedSummary);
                    DatabaseEntry dataDbt = new DatabaseEntry(new byte[0]);
                    cursor.putCurrent(dataDbt, null, null);
                }
            } finally {
                if (cursor != null) {
                    cursor.releaseBINs();
                    cursor.close();
                    cursor = null;
                }
            }
            if (ln == null) {
                /* Summary does not exist, insert it. */
                try {
                    cursor = new CursorImpl(fileSummaryDb, locker);
                    ln = new FileSummaryLN(new FileSummary());
                    ln.setTrackedSummary(trackedSummary);
                    cursor.releaseBINs();
                    cursor.putLN(new Key(keyBytes), ln, false);
                } finally {
                    if (cursor != null) {
                        cursor.close();
                        cursor = null;
                    }
                }
            }
            /*
             * Cache and return the summary object.
             * We cannot call cacheFileSummary here because the LN caches the
             * summary object in the tree, so the old value in our map has
             * already been updated.  We have to get the old value before doing
             * the update (above) and then subtract it here.
             */
            FileSummary summary = ln.getBaseSummary();
            fileSummaryMap.put(fileNumLong, summary);
            totalLogSize -= oldTotalSize;
            totalLogSize += summary.totalSize;
            return summary;
        } finally {
            if (locker != null) {
                locker.operationEnd();
            }
        }
    }

    /**
     * If the file summary db is already open, return it, otherwise attempt to
     * open it.  If the environment is read-only and the database doesn't
     * exist, return false.  If the environment is read-write the database will
     * be created if it doesn't exist.
     */
    private synchronized boolean openFileSummaryDatabase()
        throws DatabaseException {

        if (fileSummaryDb != null) {
            return true;
        }
        DbTree dbTree = env.getDbMapTree();
        Locker autoTxn = null;
        boolean operationOk = false;
        try {
            autoTxn = new AutoTxn(env, new TransactionConfig());
            DatabaseImpl db = dbTree.getDb(autoTxn,
                                           DbTree.UTILIZATION_DB_NAME,
                                           null);
            if (db == null) {
                if (env.isReadOnly()) {
                    return false;
                }
                db = dbTree.createDb(autoTxn, DbTree.UTILIZATION_DB_NAME,
                                     new DatabaseConfig(), null);
            }
            fileSummaryDb = db;
            operationOk = true;
            return true;
        } finally {
            if (autoTxn != null) {
                autoTxn.operationEnd(operationOk);
            }
        }
    }
}
