/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: UtilizationTracker.java,v 1.1 2004/11/22 18:27:58 kate Exp $
*/

package com.sleepycat.je.cleaner;

import java.util.ArrayList;
import java.util.List;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.utilint.DbLsn;

/**
 * Tracks changes to the utilization profile since that last checkpoint.
 *
 * <p>All changes to this object occur must under the log write latch.  It is
 * possible to read tracked info without holding the latch.  This is done by
 * the cleaner when selecting a file and by the checkpointer when determining
 * what FileSummaryLNs need to be written.  To read tracked info outside the
 * log write latch, call getTrackedFile or getTrackedFiles.  getLogSizeDelta
 * and activateCleaner can also be called outside the latch.</p>
 */
public class UtilizationTracker {

    private static LogEntryType[] IN_TYPES = {
        LogEntryType.LOG_IN,
        LogEntryType.LOG_BIN,
        LogEntryType.LOG_DIN,
        LogEntryType.LOG_DBIN,
    };

    private EnvironmentImpl env;
    private List files;
    private long activeFile;
    private TrackedFileSummary[] snapshot;
    private long logSizeDelta;
    private long bytesSinceActivate;
    private long cleanerBytesInterval;

    /**
     * Creates an empty tracker.
     */
    public UtilizationTracker(EnvironmentImpl env)
        throws DatabaseException {

        this.env = env;
        files = new ArrayList();
        snapshot = new TrackedFileSummary[0];
        activeFile = -1;

        cleanerBytesInterval = env.getConfigManager().getLong
            (EnvironmentParams.CLEANER_BYTES_INTERVAL);
        if (cleanerBytesInterval == 0) {
            cleanerBytesInterval = env.getConfigManager().getLong
                (EnvironmentParams.LOG_FILE_MAX) / 4;
        }
    }

    public void activateCleaner() {
        /* Cleaner may not be started yet. */
        if (env.getCleaner() != null) {
            env.getCleaner().wakeup();
            bytesSinceActivate = 0;
        }
    }

    /**
     * Returns a snapshot of the files being tracked as of the last time a
     * log entry was added.  The summary info returned is the delta since the
     * last checkpoint, not the grand totals, and is approximate since it is
     * changing in real time.  This method may be called without holding the
     * log write latch.
     *
     * <p>If files are added or removed from the list of tracked files in real
     * time, the returned array will not be changed since it is a snapshot.
     * But the objects contained in the array are live and will be updated in
     * real time under the log write latch.  The array and the objects in the
     * array should not be modified by the caller.</p>
     */
    public TrackedFileSummary[] getTrackedFiles() {
        return snapshot;
    }

    /**
     * Returns one file from the snapshot of tracked files, or null if the
     * given file number is not in the snapshot array.
     * @see #getTrackedFiles
     */
    public TrackedFileSummary getTrackedFile(long fileNum) {
        /*
         * Use a local variable to access the array since the snapshot field
         * can be changed by other threads.
         */
        TrackedFileSummary[] a = snapshot;
        for (int i = 0; i < a.length; i += 1) {
            if (a[i].getFileNumber() == fileNum) {
                return a[i];
            }
        }
        return null;
    }

    /**
     * Returns the number of bytes added to the log since the last checkpoint.
     */
    long getLogSizeDelta() {
        return logSizeDelta;
    }

    /**
     * Counts the addition of all new log entries including LNs, and returns
     * whether the cleaner should be woken.
     *
     * <p>Must be called under the log write latch.</p>
     */
    public boolean countNewLogEntry(DbLsn lsn, LogEntryType type, int size) {

        TrackedFileSummary file = getFile(lsn.getFileNumber());
        file.totalCount += 1;
        file.totalSize += size;
        if (type.isNodeType()) {
            if (inArray(type, IN_TYPES)) {
                file.totalINCount += 1;
                file.totalINSize += size;
            } else {
                file.totalLNCount += 1;
                file.totalLNSize += size;
            }
        }
        logSizeDelta += size;
        bytesSinceActivate += size;
        return (bytesSinceActivate >= cleanerBytesInterval);
    }

    /**
     * Counts a change in the obsolete status of an node, incrementing the
     * obsolete count if obsolete is true and decrementing it if obsolete is
     * false.  If type is null, we assume it is an LN node, otherwise type must
     * be an LN or IN type.
     *
     * <p>Must be called under the log write latch.</p>
     */
    public void countObsoleteNode(DbLsn lsn, LogEntryType type,
                                  boolean obsolete) {

        /* Currently we only count obsolete LNs. */
        assert type == null || !inArray(type, IN_TYPES);
        TrackedFileSummary file = getFile(lsn.getFileNumber());
        file.obsoleteLNCount += (obsolete ? 1 : -1);
    }

    /**
     * Adds changes from a given FileSummary.
     *
     * <p>Must be called under the log write latch.</p>
     */
    public void addSummary(long fileNumber, FileSummary other) {

        TrackedFileSummary file = getFile(fileNumber);
        file.add(other);
    }
        
    /**
     * Returns a tracked file for the given file number, adding an empty one
     * if the file is not already being tracked.
     *
     * <p>Must be called under the log write latch.</p>
     */
    private TrackedFileSummary getFile(long fileNum) {

        if (activeFile < fileNum) {
            activeFile = fileNum;
        }
        int size = files.size();
        for (int i = 0; i < size; i += 1) {
            TrackedFileSummary file = (TrackedFileSummary) files.get(i);
            if (file.getFileNumber() == fileNum) {
                return file;
            }
        }
        TrackedFileSummary file = new TrackedFileSummary(this, fileNum);
        files.add(file);
        takeSnapshot();
        return file;
    }

    /**
     * Called after the FileSummaryLN is written to the log during checkpoint.
     * 
     * <p>We keep the active file summary in the tracked file list, but we
     * remove older files to prevent unbounded growth of the list.</p>
     *
     * <p>Must be called under the log write latch.</p>
     */
    void resetFile(TrackedFileSummary file) {

        if (file.getFileNumber() < activeFile) {
            files.remove(file);
            takeSnapshot();
        }
        logSizeDelta -= file.totalSize;
    }

    /**
     * Takes a snapshot of the tracked file list.
     *
     * <p>Must be called under the log write latch.</p>
     */
    private void takeSnapshot() {
        /*
         * Only assign to the snapshot field with a populated array, since it
         * will be accessed by other threads.
         */
        TrackedFileSummary[] a = new TrackedFileSummary[files.size()];
        files.toArray(a);
        snapshot = a;
    }

    /**
     * Returns whether an object reference is in an array.
     */
    private boolean inArray(Object o, Object[] a) {

        for (int i = 0; i < a.length; i += 1) {
            if (a[i] == o) {
                return true;
            }
        }
        return false;
    }
}
