/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: SearchFileReader.java,v 1.1 2004/11/22 18:27:58 kate Exp $
*/

package com.sleepycat.je.log;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.entry.LogEntry;
import com.sleepycat.je.utilint.DbLsn;

/**
 * SearchFileReader searches for the a given entry type.
 */
public class SearchFileReader extends FileReader {

    private LogEntryType targetType;
    private LogEntry logEntry;

    /**
     * Create this reader to start at a given LSN.
     */
    public SearchFileReader(EnvironmentImpl env,
                            int readBufferSize, 
                            DbLsn startLsn,
                            LogEntryType targetType)
	throws IOException, DatabaseException {

        super(env, readBufferSize, true,  startLsn, 
              null, null, null);

        this.targetType = targetType;
        logEntry = targetType.getNewLogEntry();
    }

    /** 
     * @return true if this is a targetted entry.
     */
    protected boolean isTargetEntry(byte logEntryTypeNumber,
                                    byte logEntryTypeVersion) {
        return (targetType.equalsType(logEntryTypeNumber,
                                      logEntryTypeVersion));
    }
    
    /**
     * This reader instantiate the first object of a given log entry.
     */
    protected boolean processEntry(ByteBuffer entryBuffer)
        throws DatabaseException {

        logEntry.readEntry(entryBuffer);
        return true;
    }

    /**
     * @return the last object read.
     */
    public Object getLastObject() {
        return logEntry.getMainItem();
    }
}
