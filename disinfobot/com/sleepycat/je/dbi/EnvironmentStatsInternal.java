/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: EnvironmentStatsInternal.java,v 1.1 2004/11/22 18:27:59 kate Exp $
*/

package com.sleepycat.je.dbi;

import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.utilint.DbLsn;

/**
 * An internal version of EnvironmentStats, which holds a DbLsn. Used to 
 * hide DbLsn from the public api.
 */
public class EnvironmentStatsInternal extends EnvironmentStats {

    /**
     * The location in the log of the last checkpoint start.
     */
    protected DbLsn lastCheckpointStart;

    /**
     * The location in the log of the last checkpoint end.
     */
    protected DbLsn lastCheckpointEnd;

    /**
     * Resets all stats.
     */
    protected void reset() {
        super.reset();
        lastCheckpointStart = null;
        lastCheckpointEnd = null;
    }

    /**
     */
    public DbLsn getLastCheckpointEnd() {
        return lastCheckpointEnd;
    }

    /**
     */
    public DbLsn getLastCheckpointStart() {
        return lastCheckpointStart;
    }

    /**
     * @param lsn
     */
    public void setLastCheckpointEnd(DbLsn lsn) {
        lastCheckpointEnd = lsn;
    }

    /**
     * @param lsn
     */
    public void setLastCheckpointStart(DbLsn lsn) {
        lastCheckpointStart = lsn;
    }
    public void setCacheDataBytes(long cacheDataBytes) {
        this.cacheDataBytes = cacheDataBytes;
    }

    public void setNNotResident(long nNotResident) {
        this.nNotResident = nNotResident;
    }

    public void setNCacheMiss(long nCacheMiss) {
        this.nCacheMiss = nCacheMiss;
    }

    public void setNLogBuffers(int nLogBuffers) {
        this.nLogBuffers = nLogBuffers;
    }

    public void setBufferBytes(long bufferBytes) {
        this.bufferBytes = bufferBytes;
    }

    /**
     * @param val
     */
    public void setCursorsBins(int val) {
        cursorsBins = val;
    }

    /**
     * @param val
     */
    public void setDbClosedBins(int val) {
        dbClosedBins = val;
    }

    /**
     * @param val
     */
    public void setInCompQueueSize(int val) {
        inCompQueueSize = val;
    }

    /**
     * @param l
     */
    public void setLastCheckpointId(long l) {
        lastCheckpointId = l;
    }

    /**
     * @param val
     */
    public void setNCheckpoints(int val) {
        nCheckpoints = val;
    }

    /**
     * @param val
     */
    public void setNCleanerRuns(int val) {
        nCleanerRuns = val;
    }

    /**
     * @param val
     */
    public void setNCleanerDeletions(int val) {
        nCleanerDeletions = val;
    }

    /**
     * @param val
     */
    public void setNDeltaINFlush(int val) {
        nDeltaINFlush = val;
    }

    /**
     * @param val
     */
    public void setNDeltasCleaned(int val) {
        nDeltasCleaned = val;
    }

    /**
     * @param val
     */
    public void setNCleanerEntriesRead(int val) {
        nCleanerEntriesRead = val;
    }

    /**
     * @param val
     */
    public void setNEvictPasses(int val) {
        nEvictPasses = val;
    }

    /**
     */
    public void setNFSyncs(long val) {
        nFSyncs = val;
    }

    /**
     */
    public void setNFSyncRequests(long val) {
        nFSyncRequests = val;
    }

    /**
     */
    public void setNFSyncTimeouts(long val) {
        nFSyncTimeouts = val;
    }

    /**
     * @param val
     */
    public void setNFullINFlush(int val) {
        nFullINFlush = val;
    }

    /**
     * @param val
     */
    public void setNFullBINFlush(int val) {
        nFullBINFlush = val;
    }

    /**
     * @param val
     */
    public void setNINsCleaned(int val) {
        nINsCleaned = val;
    }

    /**
     * @param val
     */
    public void setNINsMigrated(int val) {
        nINsMigrated = val;
    }

    /**
     * @param val
     */
    public void setNLNsCleaned(int val) {
        nLNsCleaned = val;
    }

    /**
     * @param val
     */
    public void setNLNsDead(int val) {
        nLNsDead = val;
    }

    /**
     * @param val
     */
    public void setNLNsLocked(int val) {
        nLNsLocked = val;
    }

    /**
     * @param val
     */
    public void setNLNsMigrated(int val) {
        nLNsMigrated = val;
    }

    /**
     * @param l
     */
    public void setNNodesExplicitlyEvicted(long l) {
        nNodesExplicitlyEvicted = l;
    }

    /**
     * @param l
     */
    public void setRequiredEvictBytes(long l) {
        requiredEvictBytes = l;
    }

    /**
     * @param l
     */
    public void setNBINsStripped(long l) {
        nBINsStripped = l;
    }

    /**
     * @param l
     */
    public void setNNodesScanned(long l) {
        nNodesScanned = l;
    }

    /**
     * @param l
     */
    public void setNNodesSelected(long l) {
        nNodesSelected = l;
    }

    /**
     * @param val
     */
    public void setNonEmptyBins(int val) {
        nonEmptyBins = val;
    }

    /**
     * @param val
     */
    public void setProcessedBins(int val) {
        processedBins = val;
    }

    /**
     * @param val
     */
    public void setNRepeatFaultReads(long val) {
        nRepeatFaultReads = val;
    }

    /**
     * @param val
     */
    public void setNRepeatIteratorReads(long val) {
        nRepeatIteratorReads = val;
    }

    /**
     * @param val
     */
    public void setSplitBins(int val) {
        splitBins = val;
    }
}
