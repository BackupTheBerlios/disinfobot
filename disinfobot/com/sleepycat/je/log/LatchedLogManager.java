/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: LatchedLogManager.java,v 1.1 2004/11/22 18:27:58 kate Exp $
*/

package com.sleepycat.je.log;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.cleaner.TrackedFileSummary;
import com.sleepycat.je.cleaner.UtilizationTracker;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.utilint.DbLsn;

/**
 * The LatchedLogManager uses the latches to
 * implement critical sections.
 */
public class LatchedLogManager extends LogManager {

                                           
    /**
     * There is a single log manager per database environment.
     */
    public LatchedLogManager(EnvironmentImpl envImpl,
                            boolean readOnly)
        throws DatabaseException {

        super(envImpl, readOnly);
    }


    protected LogResult logItem(LoggableObject item,
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

        logWriteLatch.acquire();
        try {
            return logInternal(item, isProvisional, flushRequired,
                               oldNodeLsn, isDeletedNode, entrySize,
                               itemSize, marshallOutsideLatch,
                               marshalledBuffer, tracker);
        } finally {
            logWriteLatch.release();
        }
    }


    protected void flushInternal() 
        throws LogException, DatabaseException {

        logWriteLatch.acquire();
        try {
            logBufferPool.writeBufferToFile(0);
        } catch (IOException e) {
            throw new LogException(e.getMessage());
        } finally {
            logWriteLatch.release();
        }
    }

    /**
     * @see LogManager#countObsoleteLNs
     */
    public void countObsoleteLNs(DbLsn lsn1, boolean obsolete1,
                                 DbLsn lsn2, boolean obsolete2)
        throws DatabaseException {

        UtilizationTracker tracker = envImpl.getUtilizationTracker();
        logWriteLatch.acquire();
        try {
            countObsoleteLNsInternal(tracker, lsn1, obsolete1,
                                     lsn2, obsolete2);
        } finally {
            logWriteLatch.release();
        }
    }

    /**
     * @see LogManager#countObsoleteNodes
     */
    public void countObsoleteNodes(TrackedFileSummary[] summaries)
        throws DatabaseException {

        UtilizationTracker tracker = envImpl.getUtilizationTracker();
        logWriteLatch.acquire();
        try {
            countObsoleteNodesInternal(tracker, summaries);
        } finally {
            logWriteLatch.release();
        }
    }
}
