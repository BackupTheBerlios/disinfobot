/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: LogManager.java,v 1.1 2004/11/22 18:27:58 kate Exp $
*/

package com.sleepycat.je.log;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.zip.Checksum;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.RunRecoveryException;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.cleaner.TrackedFileSummary;
import com.sleepycat.je.cleaner.UtilizationTracker;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.EnvironmentStatsInternal;
import com.sleepycat.je.latch.Latch;
import com.sleepycat.je.log.entry.LogEntry;
import com.sleepycat.je.utilint.Adler32;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.Tracer;

/**
 * The LogManager supports reading and writing to the JE log.
 */
abstract public class LogManager {

    // no-op loggable object
    private static final String DEBUG_NAME = LogManager.class.getName();
    private static final boolean DEBUG = false;
    
    /*
     * Log entry header field sizes
     */
    static final int HEADER_BYTES = 14;            // size of entry header
    private static final int CHECKSUM_BYTES = 4;   // size of checksum field
    private static final int PREV_BYTES = 4;       // size of previous field
    static final int HEADER_CONTENT_BYTES =
        HEADER_BYTES - CHECKSUM_BYTES;
    static final int HEADER_CHECKSUM_OFFSET = 0;
    static final int HEADER_ENTRY_TYPE_OFFSET = 4;
    static final int HEADER_VERSION_OFFSET = 5;
    static final int HEADER_PREV_OFFSET = 6;
    static final int HEADER_SIZE_OFFSET = 10;

    protected LogBufferPool logBufferPool; // log buffers
    protected Latch logWriteLatch;           // synchronizes log writes
    private boolean doChecksumOnRead;      // if true, do checksum on read
    private FileManager fileManager;       // access to files
    private CheckpointMonitor checkpointMonitor;
    protected EnvironmentImpl envImpl;
    private boolean readOnly;
    private int readBufferSize; // how many bytes to read when faulting in.

    /* Stats */

    /* 
     * Number of times we have to repeat a read when we fault in an object
     * because the initial read was too small.    
     */
    private int nRepeatFaultReads; 

    /**
     * There is a single log manager per database environment.
     */
    public LogManager(EnvironmentImpl envImpl,
                      boolean readOnly)
        throws DatabaseException {

        // Set up log buffers
        this.envImpl = envImpl;
        this.fileManager = envImpl.getFileManager();
        DbConfigManager configManager = envImpl.getConfigManager();
	this.readOnly = readOnly;
        logBufferPool = new LogBufferPool(fileManager, envImpl);

        // See if we're configured to do a checksum when reading in objects
        doChecksumOnRead =
	    configManager.getBoolean(EnvironmentParams.LOG_CHECKSUM_READ);
        
        logWriteLatch = new Latch(DEBUG_NAME, envImpl);
        readBufferSize =
	    configManager.getInt(EnvironmentParams.LOG_FAULT_READ_SIZE);
        checkpointMonitor = new CheckpointMonitor(envImpl);
    }

    /*
     * Writing to the log
     */

    /**
     * Log this single object and force a flush of the log files.
     * @param item object to be logged
     * @return DbLsn of the new log entry
     */
    public DbLsn logForceFlush(LoggableObject item)
	throws DatabaseException {

        return log(item, false, true, null, false);
    }

    /**
     * Write a log entry.
     * @return DbLsn of the new log entry
     */
    public DbLsn log(LoggableObject item) 
	throws DatabaseException {

        return log(item, false, false, null, false);
    }

    /**
     * Write a log entry.
     * @return DbLsn of the new log entry
     */
    public DbLsn log(LoggableObject item, boolean isProvisional,
                     DbLsn oldNodeLsn, boolean isDeletedNode)
	throws DatabaseException {

        return log(item, isProvisional, false, oldNodeLsn, isDeletedNode);
    }

    /**
     * Write a log entry.
     * @param item is the item to be logged.
     * @param isProvisional true if this entry should not be read during
     * recovery.
     * @param flushRequired is true to flush the log after adding the item.
     * @param oldNodeLsn is the previous version of the node to be counted as
     * obsolete, or null if the item is not a node or has no old LSN.
     * @param isDeletedNode is true if the item is a deleted node and should
     * therefore be counted as obsolete.
     * @return DbLsn of the new log entry
     */
    private DbLsn log(LoggableObject item,
                      boolean isProvisional,
                      boolean flushRequired,
                      DbLsn oldNodeLsn,
                      boolean isDeletedNode)
	throws DatabaseException {

	if (readOnly) {
	    return null;
	}

        int itemSize = item.getLogSize();
        int entrySize = itemSize + HEADER_BYTES;

        boolean marshallOutsideLatch = item.marshallOutsideWriteLatch();
        ByteBuffer marshalledBuffer = null;
        UtilizationTracker tracker = envImpl.getUtilizationTracker();
        LogResult logResult = null;

        try {

            /* 
             * If possible, marshall this item outside the log write
             * latch to allow greater concurrency by shortening the
             * write critical section.
             */
            if (marshallOutsideLatch) {
		marshalledBuffer = marshallIntoBuffer(item,
                                                      itemSize,
                                                      isProvisional,
                                                      entrySize);
            }

            logResult = logItem(item, isProvisional, flushRequired,
                                oldNodeLsn, isDeletedNode, entrySize,
                                itemSize, marshallOutsideLatch,
                                marshalledBuffer, tracker);

        } catch (BufferOverflowException e) {

            /* 
             * A BufferOverflowException may be seen when a thread is
             * interrupted in the middle of the log and the nio direct buffer
             * is mangled is some way by the NIO libraries. JE applications
             * should refrain from using thread interrupt as a thread
             * communications mechanism because nio behavior in the face of
             * interrupts is uncertain. See SR [#10463].
             *
             * One way or another, this type of io exception leaves us in an
             * unworkable state, so throw a run recovery exception.
             */
            throw new RunRecoveryException(envImpl, e);
        } catch (IOException e) {

            /*
             * Other IOExceptions, such as out of disk conditions, should
             * notify the application but leave the environment in workable
             * condition.
             */
            throw new DatabaseException(Tracer.getStackTrace(e), e);
        }

        /*
         * Finish up business outside of the log write latch critical section.
         */

        /* 
	 * If this logged object needs to be fsynced, do so now using the group
	 * commit mecnanism.
         */
        if (flushRequired) {
            fileManager.groupSync();
        }

        /* 
         * Periodically, as a function of how much data is written, ask the
	 * checkpointer or the cleaner to wake up.
         */
        if (logResult.wakeupCheckpointer) {
            checkpointMonitor.activate();
        }
        if (logResult.wakeupCleaner) {
            tracker.activateCleaner();
        }

        return logResult.currentLsn;
    }

    abstract protected LogResult logItem(LoggableObject item,
                                         boolean isProvisional,
                                         boolean flushRequired,
                                         DbLsn oldNodeLsn,
                                         boolean isDeletedNode,
                                         int entrySize,
                                         int itemSize,
                                         boolean marshallOutsideLatch,
                                         ByteBuffer marshalledBuffer,
                                         UtilizationTracker tracker)
        throws IOException, DatabaseException;

    /**
     * Called within the log write critical section. 
     */
    protected LogResult logInternal(LoggableObject item,
                                    boolean isProvisional,
                                    boolean flushRequired,
                                    DbLsn oldNodeLsn,
                                    boolean isDeletedNode,
                                    int entrySize,
                                    int itemSize,
                                    boolean marshallOutsideLatch,
                                    ByteBuffer marshalledBuffer,
                                    UtilizationTracker tracker)
        throws IOException, DatabaseException {

        /* 
         * Get the next free slot in the log, under the log write latch.  Bump
         * the LSN values, which gives us a valid previous pointer, which is
         * part of the log entry header. That's why doing the checksum must be
         * in the log write latch -- we need to bump the lsn first, and bumping
         * the lsn must be done within the log write latch.
         */
        boolean flippedFile = fileManager.bumpLsn(entrySize);
        DbLsn currentLsn = fileManager.getLastUsedLsn();
        LogEntryType entryType = item.getLogType();
            
        /* 
         * Maintain utilization information for the cleaner: Count before
         * marshalling a FileSummaryLN into the log buffer so a FileSummaryLN
         * counts itself.
         */
        boolean wakeupCleaner =
            tracker.countNewLogEntry(currentLsn, entryType, entrySize);

        if (isDeletedNode) {
            tracker.countObsoleteNode(currentLsn, entryType, true);
        }
        if (oldNodeLsn != null) {
            tracker.countObsoleteNode(oldNodeLsn, entryType, true);
        }

        /* 
         * This item must be marshalled within the log write latch.
         */
        if (!marshallOutsideLatch) {
            marshalledBuffer = marshallIntoBuffer(item,
                                                  itemSize,
                                                  isProvisional,
                                                  entrySize);
        }

	/*
	 * Ask for a log buffer suitable for holding this new entry.  If the
	 * current log buffer is full, or if we flipped into a new file, write
	 * it to disk and get a new, empty log buffer to use. The returned
	 * buffer will be latched for write.
	 */
	ByteBuffer useBuffer =
	    logBufferPool.getWriteBuffer(entrySize, flippedFile);

	/* Add checksum to entry. */
	marshalledBuffer =
	    addPrevOffsetAndChecksum(marshalledBuffer,
				     fileManager.getPrevEntryOffset(),
				     entrySize);

	/* Copy marshalled object into write buffer. */
	useBuffer.put(marshalledBuffer);
        
	/* 
	 * Tell the log buffer pool that we finished the write.  Record the
	 * LSN against this logbuffer, and write the buffer to disk if
	 * needed.
	 */
	logBufferPool.writeCompleted(currentLsn, flushRequired);

        /*
         * If the txn is not null, the first item is an LN. Update the txn with
         * info about the latest LSN. Note that this has to happen within the
         * log write latch.
         */
        item.postLogWork(currentLsn);

        boolean wakeupCheckpointer =
            checkpointMonitor.recordLogWrite(entrySize, item);

        return new LogResult(currentLsn, wakeupCheckpointer, wakeupCleaner);
    }

    /**
     * Serialize a loggable object into this buffer.
     */
    private ByteBuffer marshallIntoBuffer(LoggableObject item,
                                          int itemSize,
                                          boolean isProvisional,
                                          int entrySize)
	throws DatabaseException {

        ByteBuffer destBuffer = ByteBuffer.allocate(entrySize);

        // Reserve 4 bytes at the head for the checksum.
        destBuffer.position(CHECKSUM_BYTES);

        // Write the header.
        writeHeader(destBuffer, item.getLogType(), itemSize, isProvisional);

        // Put the entry in.
        item.writeToLog(destBuffer);
        return destBuffer;
    }

    private ByteBuffer addPrevOffsetAndChecksum(ByteBuffer destBuffer,
                                                long lastOffset,
                                                int entrySize) {

        Checksum checksum = new Adler32();
            
        /* Add the prev pointer */
        destBuffer.position(HEADER_PREV_OFFSET);
        LogUtils.writeUnsignedInt(destBuffer, lastOffset);

        /* Now calculate the checksum and write it into the buffer. */
        checksum.update(destBuffer.array(), CHECKSUM_BYTES,
                        (entrySize - CHECKSUM_BYTES));
        destBuffer.position(0);
        LogUtils.putUnsignedInt(destBuffer, checksum.getValue());

        /* Leave this buffer ready for copying into another buffer. */
        destBuffer.position(0);

        return destBuffer;
    }

    /**
     * Serialize a loggable object into this buffer. Return it ready for a
     * copy.
     */
    ByteBuffer putIntoBuffer(LoggableObject item,
                             int itemSize,
                             long prevLogEntryOffset,
                             boolean isProvisional,
                             int entrySize)
	throws DatabaseException {

        ByteBuffer destBuffer =
	    marshallIntoBuffer(item, itemSize, isProvisional, entrySize);
        destBuffer.position(0);
        return addPrevOffsetAndChecksum(destBuffer, 0, entrySize);
    }

    /**
     * Helper to write the common entry header.
     * @param destBuffer destination
     * @param item object being logged
     * @param itemSize We could ask the item for this, but are passing it
     * as a parameter for efficiency, because it's already available
     */
    private void writeHeader(ByteBuffer destBuffer,
                             LogEntryType itemType,
                             int itemSize,
                             boolean isProvisional) {
        // log entry type
        byte typeNum = itemType.getTypeNum();
        destBuffer.put(typeNum);

        // version
        byte version = itemType.getVersion();
        if (isProvisional)
            version = LogEntryType.setProvisional(version);
        destBuffer.put(version);

        // entry size
        destBuffer.position(HEADER_SIZE_OFFSET);
        LogUtils.writeInt(destBuffer, itemSize);
    }

    /*
     * Reading from the log.
     */

    /**
     * Instantiate all the objects in the log entry at this LSN.
     * @param lsn location of entry in log.
     * @return log entry that embodies all the objects in the log entry.
     */
    public LogEntry getLogEntry(DbLsn lsn) 
        throws DatabaseException {

        /*
         * Get a log source for the log entry which provides an abstraction
         * that hides whether the entry is in a buffer or on disk. Will
         * register as a reader for the buffer or the file, which will take a
         * latch if necessary.
         */
        LogSource logSource = getLogSource(lsn);

        /* Read the log entry from the log source. */
        return getLogEntryFromLogSource(lsn, logSource);
    }

    LogEntry getLogEntry(DbLsn lsn, RandomAccessFile file)
        throws DatabaseException {

        return getLogEntryFromLogSource(lsn,
					new FileSource(file, readBufferSize));
    }

    /**
     * Instantiate all the objects in the log entry at this lsn. This will
     * release the log source at the first opportunity.
     *
     * @param lsn location of entry in log
     * @return log entry that embodies all the objects in the log entry
     */
    private LogEntry getLogEntryFromLogSource(DbLsn lsn,
                                              LogSource logSource) 
        throws DatabaseException {

        try {

            /*
             * Read the log entry header into a byte buffer. Be sure to read it
             * in the order that it was written, and with the same marshalling!
             * Ideally, entry header read/write would be encapsulated in a
             * single class, but we don't want to have to instantiate a new
             * object in the critical path here.
	     * XXX - false economy, change.
             */
            long fileOffset = lsn.getFileOffset();
            ByteBuffer entryBuffer = logSource.getBytes(fileOffset);

            /* Read the checksum to move the buffer forward. */
            ChecksumValidator validator = null;
            long storedChecksum = LogUtils.getUnsignedInt(entryBuffer);
            if (doChecksumOnRead) {
                validator = new ChecksumValidator();
                validator.update(envImpl, entryBuffer, HEADER_CONTENT_BYTES);
            }
            if (DEBUG) {
                System.out.println("storedChecksum = " + storedChecksum +
                                   " pos = " + entryBuffer.position());
            }

            /* Read the header. */
            byte loggableType = entryBuffer.get(); // log entry type
            byte version = entryBuffer.get();      // version
            /* Read the size, skipping over the prev offset. */
            entryBuffer.position(entryBuffer.position() + PREV_BYTES);
            int itemSize = LogUtils.readInt(entryBuffer);

            /*
             * Now that we know the size, read the rest of the entry
             * if the first read didn't get enough.
             */
            if (entryBuffer.remaining() < itemSize) {
                entryBuffer = logSource.getBytes(fileOffset + HEADER_BYTES,
                                                 itemSize);
                nRepeatFaultReads++;
            }

            /*
             * Do entry validation. Run checksum before checking the entry
             * type, it will be the more encompassing error.
             */
            if (doChecksumOnRead) {
                /* Check the checksum first. */
                validator.update(envImpl, entryBuffer, itemSize);
                validator.validate(envImpl, storedChecksum, lsn);
            }

            assert LogEntryType.isValidType(loggableType):
                "Read non-valid log entry type: " + loggableType;

            /* Read the entry. */
            LogEntry logEntry = 
                LogEntryType.findType(loggableType, version).getNewLogEntry();
            logEntry.readEntry(entryBuffer);

            /* 
             * Done with the log source, release in the finally clause.  Note
             * that the buffer we get back from logSource is just a duplicated
             * buffer, where the position and state are copied but not the
             * actual data. So we must not release the logSource until we are
             * done marshalling the data from the buffer into the object
             * itself.
             */
            return logEntry;
        } catch (DatabaseException e) {
            /* 
	     * Propagate DatabaseExceptions, we want to preserve any subtypes *
             * for downstream handling.
             */
            throw e;
        } catch (Exception e) {
            throw new DatabaseException(e);
        } finally {
            if (logSource != null) {
                logSource.release();
            }
        }
    }

    /**
     * Fault in the first object in the log entry log entry at this LSN.
     * @param lsn location of object in log
     * @return the object in the log
     */
    public Object get(DbLsn lsn)
        throws DatabaseException {

        LogEntry entry = getLogEntry(lsn);
        return entry.getMainItem();
    }

    /**
     * Find the LSN, whether in a file or still in the log buffers.
     */
    private LogSource getLogSource(DbLsn lsn)
        throws DatabaseException {

        /*
	 * First look in log to see if this LSN is still in memory.
	 */
        LogBuffer logBuffer = logBufferPool.getReadBuffer(lsn);

        if (logBuffer == null) {
            try {
                /* Not in the in-memory log -- read it off disk. */
                return new FileHandleSource
                    (fileManager.getFileHandle(lsn.getFileNumber()),
                     readBufferSize);
            } catch (LogFileNotFoundException e) {
                /* Add LSN to exception message. */
                throw new LogFileNotFoundException(lsn.getNoFormatString() +
                                                   ' ' + e.getMessage());
            }
        } else {
            return logBuffer;
        }
    }

    /**
     * Flush all log entries, sync the log file.
     */
    public void flush()
	throws DatabaseException {

	if (readOnly) {
	    return;
	}

        flushInternal();
        fileManager.syncLogEnd();
    }

    abstract protected void flushInternal()
        throws LogException, DatabaseException;


    public void loadStats(StatsConfig config,
                          EnvironmentStatsInternal stats) 
        throws DatabaseException {

        stats.setNRepeatFaultReads(nRepeatFaultReads);
        if (config.getClear()) {
            nRepeatFaultReads = 0;
        }

        logBufferPool.loadStats(config, stats);
        fileManager.loadStats(config, stats);
    }

    /**
     * Count LNs as obsolete under the log write latch.  This is only used
     * during txn abort processing.  Two LNs may be counted, so that we only
     * need to acquire the latch once.  This is done here because the log write
     * latch is managed here, and all utilization counting must be performed
     * under the log write latch.
     */
    abstract public void countObsoleteLNs(DbLsn lsn1, boolean obsolete1,
                                          DbLsn lsn2, boolean obsolete2)
        throws DatabaseException;

    protected void countObsoleteLNsInternal(UtilizationTracker tracker,
                                            DbLsn lsn1, boolean obsolete1,
                                            DbLsn lsn2, boolean obsolete2)
        throws DatabaseException {
        
        if (lsn1 != null) {
            tracker.countObsoleteNode(lsn1, null, obsolete1);
        }
        if (lsn2 != null) {
            tracker.countObsoleteNode(lsn2, null, obsolete2);
        }
    }

    /**
     * Counts file summary info under the log write latch.  This is only used
     * during txn commit processing.
     */
    abstract public void countObsoleteNodes(TrackedFileSummary[] summaries)
        throws DatabaseException;

    protected void countObsoleteNodesInternal(UtilizationTracker tracker,
                                              TrackedFileSummary[] summaries)
        throws DatabaseException {
        
        for (int i = 0; i < summaries.length; i += 1) {
            TrackedFileSummary summary = summaries[i];
            tracker.addSummary(summary.getFileNumber(), summary);
        }
    }

    /** 
     * LogResult holds the multivalue return from logInternal.
     */
    static class LogResult {
        DbLsn currentLsn;
        boolean wakeupCheckpointer;
        boolean wakeupCleaner;

        LogResult(DbLsn currentLsn,
                  boolean wakeupCheckpointer,
                  boolean wakeupCleaner) {
            this.currentLsn = currentLsn;
            this.wakeupCheckpointer = wakeupCheckpointer;
            this.wakeupCleaner = wakeupCleaner;
        }
    }
}
