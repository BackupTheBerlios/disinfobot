/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: UtilizationFileReader.java,v 1.1 2004/11/22 18:27:58 kate Exp $
*/

package com.sleepycat.je.log;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.cleaner.UtilizationTracker;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.entry.LNLogEntry;
import com.sleepycat.je.tree.FileSummaryLN;
import com.sleepycat.je.utilint.DbLsn;

/**
 * Counts log entries in the UtilizationTracker, without having to instantiate
 * the loggable objects.
 */
public class UtilizationFileReader extends FileReader {

    private LNLogEntry targetLogEntry;
    private UtilizationTracker tracker;

    /**
     * Create this reader to start at a given LSN.
     */
    public UtilizationFileReader(EnvironmentImpl env,
                                 int readBufferSize, 
                                 DbLsn startLsn,
                                 DbLsn finishLsn,
                                 DbLsn endOfFileLsn) 
        throws IOException, DatabaseException {

        super(env, readBufferSize, true,
              startLsn, null,
              endOfFileLsn, finishLsn);

        targetLogEntry = (LNLogEntry)
            LogEntryType.LOG_FILESUMMARYLN.getNewLogEntry();
        tracker = env.getUtilizationTracker();
    }

    /**
     * Count the log entry, but only return true for a LOG_FILESUMMARYLN.
     */
    protected boolean isTargetEntry(byte logEntryTypeNumber,
                                    byte logEntryTypeVersion) {

        LogEntryType type = LogEntryType.findType(logEntryTypeNumber,
                                                  logEntryTypeVersion);
        assert type != null;
        tracker.countNewLogEntry(getLastLsn(), type,
                                 LogManager.HEADER_BYTES + currentEntrySize);
        return logEntryTypeNumber ==
               LogEntryType.LOG_FILESUMMARYLN.getTypeNum();
    }

    /**
     * Returns the file number -- the key of the FileSummaryLN record.
     */
    public long getFileNumber() {
        byte[] keyBytes = targetLogEntry.getKey().getKey();
        return FileSummaryLN.bytesToFileNumber(keyBytes);
    }

    /**
     * Returns the FileSummaryLN last read.
     */
    public FileSummaryLN getFileSummaryLN() {
        return (FileSummaryLN) targetLogEntry.getLN();
    }

    /**
     * Read a file summary LN log entry.
     */
    protected boolean processEntry(ByteBuffer entryBuffer)
        throws DatabaseException {

        targetLogEntry.readEntry(entryBuffer);
        return true;
    }
}
