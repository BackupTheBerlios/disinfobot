/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: CheckpointFileReader.java,v 1.1 2004/11/22 18:27:58 kate Exp $
*/

package com.sleepycat.je.log;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.entry.LogEntry;
import com.sleepycat.je.utilint.DbLsn;

/**
 * CheckpointFileReader searches for root and checkpoint entries.
 */
public class CheckpointFileReader extends FileReader {
    /* Status about the last entry. */
    private boolean isRoot;
    private boolean isCheckpoint;

    /* LogEntryReaders know how to instantiate the objects in a log entry. */
    private LogEntry rootLogEntryReader;
    private LogEntry checkpointLogEntryReader;
    private LogEntry useLogEntryReader;

    /**
     * Create this reader to start at a given LSN.
     */
    public CheckpointFileReader(EnvironmentImpl env,
                                int readBufferSize, 
                                boolean forward,
                                DbLsn startLsn,
                                DbLsn finishLsn,
                                DbLsn endOfFileLsn) 
        throws IOException, DatabaseException {

        super(env, readBufferSize, forward,
              startLsn,  null,
              endOfFileLsn, finishLsn);
        rootLogEntryReader = LogEntryType.LOG_ROOT.getNewLogEntry();
        checkpointLogEntryReader =
            LogEntryType.LOG_CKPT_END.getNewLogEntry();
    }

    /** 
     * @return true if this is a targetted entry.
     */
    protected boolean isTargetEntry(byte logEntryTypeNumber,
                                    byte logEntryTypeVersion) {
        boolean isTarget = false;
        isRoot = false;
        isCheckpoint = false;
        if (LogEntryType.LOG_CKPT_END.equalsType(logEntryTypeNumber,
                                                   logEntryTypeVersion)) {
            isTarget = true;
            isCheckpoint = true;
            useLogEntryReader = checkpointLogEntryReader;
        } else if (LogEntryType.LOG_ROOT.equalsType(logEntryTypeNumber,
                                                      logEntryTypeVersion)) {
            isTarget = true;
            isRoot = true;
            useLogEntryReader = rootLogEntryReader;
        }
        return isTarget;
    }
    
    /**
     * This reader instantiate the first object of a given log entry
     */
    protected boolean processEntry(ByteBuffer entryBuffer)
        throws DatabaseException {

        useLogEntryReader.readEntry(entryBuffer);
        return true;
    }

    /**
     * @return the last object read
     */
    public Object getLastObject() {
        return useLogEntryReader.getMainItem();
    }

    /**
     * @return true if last entry was a root entry.
     */
    public boolean isRoot() {
        return isRoot;
    }

    /**
     * @return true if last entry was a checkpoint end entry.
     */
    public boolean isCheckpoint() {
        return isCheckpoint;
    }
}
