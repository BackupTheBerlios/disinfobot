/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: EnvironmentStats.java,v 1.1 2004/11/22 18:27:58 kate Exp $
*/

package com.sleepycat.je;

/**
 * Javadoc for this public class is generated
 * via the doc templates in the doc_src directory.
 */
public class EnvironmentStats {
    /* INCompressor */

    /**
     * The number of bins encountered by the INCompressor that were split
     * between the time they were put on the compressor queue and when
     * the compressor ran.
     */
    protected int splitBins;

    /**
     * The number of bins encountered by the INCompressor that had their
     * database closed between the time they were put on the
     * compressor queue and when the compressor ran.
     */
    protected int dbClosedBins;

    /**
     * The number of bins encountered by the INCompressor that had cursors
     * referring to them when the compressor ran.
     */
    protected int cursorsBins;

    /**
     * The number of bins encountered by the INCompressor that were
     * not actually empty when the compressor ran.
     */
    protected int nonEmptyBins;

    /**
     * The number of bins that were successfully processed by the IN
     * Compressor.
     */
    protected int processedBins;

    /**
     * The number of entries in the INCompressor queue when the getStats()
     * call was made.
     */
    protected int inCompQueueSize;

    /* Evictor */

    /**
     * The number of passes made to the evictor.
     */
    protected int nEvictPasses;

    /**
     * The accumulated number of nodes selected to evict.
     */
    protected long nNodesSelected;

    /**
     * The accumulated number of nodes scanned in order to select the
     * eviction set.
     */
    protected long nNodesScanned;

    /**
     * The accumulated number of nodes evicted.
     */
    protected long nNodesExplicitlyEvicted;

    /**
     * The number of BINs stripped by the evictor.
     */
    protected long nBINsStripped;

    /**
     * The number of bytes we need to evict in order to get under budget.
     */
    protected long requiredEvictBytes;

    /* Checkpointer */

    /**
     * The total number of checkpoints run so far.
     */
    protected int nCheckpoints;

    /**
     * The Id of the last checkpoint.
     */
    protected long lastCheckpointId;

    /**
     * The accumulated number of full INs flushed to the log.
     */
    protected int nFullINFlush;

    /**
     * The accumulated number of full BINs flushed to the log.
     */
    protected int nFullBINFlush;

    /**
     * The accumulated number of Delta INs flushed to the log.
     */
    protected int nDeltaINFlush;

    /* Cleaner */

    /** The number of cleaner runs this session. */
    protected int nCleanerRuns;

    /** The number of cleaner file deletions this session. */
    protected int nCleanerDeletions;

    /**
     * The accumulated number of INs cleaned.
     */
    protected int nINsCleaned;

    /**
     * The accumulated number of INs migrated.
     */
    protected int nINsMigrated;

    /**
     * The accumulated number of LNs cleaned.
     */
    protected int nLNsCleaned;

    /**
     * The accumulated number of LNs that were not found in the tree anymore
     * (deleted).
     */
    protected int nLNsDead;

    /**
     * The accumulated number of LNs encountered that were locked.
     */
    protected int nLNsLocked;

    /**
     * The accumulated number of LNs encountered that were migrated forward
     * in the log.
     */
    protected int nLNsMigrated;

    /**
     * The accumulated number of delta BINs that were cleaned.
     */
    protected int nDeltasCleaned;

    /**
     * The accumulated number of log entries read by the cleaner.
     */
    protected int nCleanerEntriesRead;

    /*
     * Cache
     */
    protected long cacheDataBytes; // part of cache consumed by data, in bytes
    protected long nNotResident;   // had to be instantiated from an lsn
    protected long nCacheMiss;     // had to retrieve from disk
    protected int  nLogBuffers;    // number of existing log buffers
    protected long bufferBytes;    // cache consumed by the log buffers, 
                                   // in bytes

    /*
     * Log activity
     */
    protected long nFSyncs;   // Number of fsyncs issued. May be less than
                              // nFSyncRequests because of group commit
    protected long nFSyncRequests; // Number of fsyncs requested. 
    protected long nFSyncTimeouts; // Number of group fsync requests that
                                   // turned into singleton fsyncs.
    /* 
     * Number of reads which had to be repeated when faulting in an
     * object from disk because the read chunk size controlled by
     * je.log.faultReadSize is too small.
     */
    protected long nRepeatFaultReads; 

    /* 
     * Number of times we try to read a log entry larger than the read
     * buffer size and can't grow the log buffer to accomodate the large
     * object. This happens during scans of the log during activities like
     * environment open or log cleaning. Implies that the the read
     * chunk size controlled by je.log.iteratorReadSize is too small.
     */
    protected long nRepeatIteratorReads;
             
    
    /**
     * Constructs a new EnvironmentStats instance and initializes all stats.
     */
    protected EnvironmentStats() {
        reset();
    }

    /**
     * Resets all stats.
     */
    protected void reset() {
        // InCompressor
        splitBins = 0;
        dbClosedBins = 0;
        cursorsBins = 0;
        nonEmptyBins = 0;
        processedBins = 0;
        inCompQueueSize = 0;

        // Evictor
        nEvictPasses = 0;
        nNodesSelected = 0;
        nNodesScanned = 0;
        nNodesExplicitlyEvicted = 0;
        nBINsStripped = 0;
        requiredEvictBytes = 0;

        // Checkpointer
        nCheckpoints = 0;
        lastCheckpointId = 0;
        nFullINFlush = 0;
        nFullBINFlush = 0;
        nDeltaINFlush = 0;

        // Cleaner
        nCleanerRuns = 0;
        nCleanerDeletions = 0;
        nINsCleaned = 0;
        nINsMigrated = 0;
        nLNsCleaned = 0;
        nLNsDead = 0;
        nLNsLocked = 0;
        nLNsMigrated = 0;
        nDeltasCleaned = 0;
        nCleanerEntriesRead = 0;

        // Cache
        cacheDataBytes = 0;
        nNotResident = 0;
        nCacheMiss = 0;
        nLogBuffers = 0;
        bufferBytes = 0;

        // Log
        nFSyncs = 0;
        nFSyncRequests = 0;
        nFSyncTimeouts = 0;
        nRepeatFaultReads = 0;
        nRepeatIteratorReads = 0;
    }

    /**
     */
    public long getBufferBytes() {
        return bufferBytes;
    }

    /**
     */
    public int getCursorsBins() {
        return cursorsBins;
    }

    /**
     */
    public int getDbClosedBins() {
        return dbClosedBins;
    }

    /**
     */
    public int getInCompQueueSize() {
        return inCompQueueSize;
    }

    /**
     */
    public long getLastCheckpointId() {
        return lastCheckpointId;
    }

    /*
     */
    public long getNCacheMiss() {
        return nCacheMiss;
    }

    /**
     */
    public int getNCheckpoints() {
        return nCheckpoints;
    }

    /**
     */
    public int getNCleanerRuns() {
        return nCleanerRuns;
    }

    /**
     */
    public int getNCleanerDeletions() {
        return nCleanerDeletions;
    }

    /**
     */
    public int getNDeltaINFlush() {
        return nDeltaINFlush;
    }

    /**
     */
    public int getNDeltasCleaned() {
        return nDeltasCleaned;
    }

    /**
     */
    public int getNCleanerEntriesRead() {
        return nCleanerEntriesRead;
    }

    /**
     */
    public int getNEvictPasses() {
        return nEvictPasses;
    }

    /**
     */
    public long getNFSyncs() {
        return nFSyncs;
    }

    /**
     */
    public long getNFSyncRequests() {
        return nFSyncRequests;
    }

    /**
     */
    public long getNFSyncTimeouts() {
        return nFSyncTimeouts;
    }

    /**
     */
    public int getNFullINFlush() {
        return nFullINFlush;
    }

    /**
     */
    public int getNFullBINFlush() {
        return nFullBINFlush;
    }

    /**
     */
    public int getNINsCleaned() {
        return nINsCleaned;
    }

    /**
     */
    public int getNINsMigrated() {
        return nINsMigrated;
    }

    /**
     */
    public int getNLNsCleaned() {
        return nLNsCleaned;
    }

    /**
     */
    public int getNLNsDead() {
        return nLNsDead;
    }

    /**
     */
    public int getNLNsLocked() {
        return nLNsLocked;
    }

    /**
     */
    public int getNLNsMigrated() {
        return nLNsMigrated;
    }

    /**
     */
    public int getNLogBuffers() {
        return nLogBuffers;
    }

    /**
     */
    public long getNNodesExplicitlyEvicted() {
        return nNodesExplicitlyEvicted;
    }

    /**
     */
    public long getNBINsStripped() {
        return nBINsStripped;
    }

    /**
     */
    public long getRequiredEvictBytes() {
        return requiredEvictBytes;
    }

    /**
     */
    public long getNNodesScanned() {
        return nNodesScanned;
    }

    /**
     */
    public long getNNodesSelected() {
        return nNodesSelected;
    }

    /**
     */
    public long getCacheTotalBytes() {
        return cacheDataBytes + bufferBytes;
    }

    /**
     */
    public long getCacheDataBytes() {
        return cacheDataBytes;
    }

    /**
     */
    public long getNNotResident() {
        return nNotResident;
    }

    /**
     */
    public int getNonEmptyBins() {
        return nonEmptyBins;
    }

    /**
     */
    public int getProcessedBins() {
        return processedBins;
    }

    /**
     */
    public long getNRepeatFaultReads() {
        return nRepeatFaultReads;
    }

    /**
     */
    public long getNRepeatIteratorReads() {
        return nRepeatFaultReads;
    }

    /**
     */
    public int getSplitBins() {
        return splitBins;
    }

    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("splitBins=").append(splitBins).append('\n');
        sb.append("dbClosedBins=").append(dbClosedBins).append('\n');
        sb.append("cursorsBins=").append(cursorsBins).append('\n');
        sb.append("nonEmptyBins=").append(nonEmptyBins).append('\n');
        sb.append("processedBins=").append(processedBins).append('\n');
        sb.append("inCompQueueSize=").append(inCompQueueSize).append('\n');

        // Evictor
        sb.append("nEvictPasses=").append(nEvictPasses).append('\n');
        sb.append("nNodesSelected=").append(nNodesSelected).append('\n');
        sb.append("nNodesScanned=").append(nNodesScanned).append('\n');
        sb.append("nNodesExplicitlyEvicted=").append(nNodesExplicitlyEvicted).append('\n');
        sb.append("nBINsStripped=").append(nBINsStripped);
        sb.append("requiredEvictBytes=").append(requiredEvictBytes);

        // Checkpointer
        sb.append("nCheckpoints=").append(nCheckpoints).append('\n');
        sb.append("lastCheckpointId=").append(lastCheckpointId).append('\n');
        sb.append("nFullINFlush=").append(nFullINFlush).append('\n');
        sb.append("nFullBINFlush=").append(nFullBINFlush).append('\n');
        sb.append("nDeltaINFlush=").append(nDeltaINFlush).append('\n');

        // Cleaner
        sb.append("nCleanerRuns=").append(nCleanerRuns).append('\n');
        sb.append("nCleanerDeletions=").append(nCleanerDeletions).append('\n');
        sb.append("nINsCleaned=").append(nINsCleaned).append('\n');
        sb.append("nINsMigrated=").append(nINsMigrated).append('\n');
        sb.append("nLNsCleaned=").append(nLNsCleaned).append('\n');
        sb.append("nLNsDead=").append(nLNsDead).append('\n');
        sb.append("nLNsLocked=").append(nLNsLocked).append('\n');
        sb.append("nLNsMigrated=").append(nLNsMigrated).append('\n');
        sb.append("nDeltasCleaned=").append(nDeltasCleaned).append('\n');
        sb.append("nCleanerEntriesRead=").append(nCleanerEntriesRead).append('\n');

        // Cache
        sb.append("nNotResident=").append(nNotResident).append('\n');
        sb.append("nCacheMiss=").append(nCacheMiss).append('\n');
        sb.append("nLogBuffers=").append(nLogBuffers).append('\n');
        sb.append("bufferBytes=").append(bufferBytes).append('\n');
        sb.append("cacheDataBytes=").append(cacheDataBytes).append('\n');
        sb.append("cacheTotalBytes=").append(getCacheTotalBytes()).
            append('\n');
        sb.append("nFSyncs=").append(nFSyncs).append('\n');
        sb.append("nFSyncRequests=").append(nFSyncRequests).append('\n');
        sb.append("nFSyncTimeouts=").append(nFSyncTimeouts).append('\n');
        sb.append("nRepeatFaultReads=").append(nRepeatFaultReads).append('\n');
        sb.append("nRepeatIteratorReads=").
            append(nRepeatIteratorReads).append('\n');

        return sb.toString();
    }
}
