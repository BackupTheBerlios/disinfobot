/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: LoggableObject.java,v 1.1 2004/11/22 18:27:58 kate Exp $
*/

package com.sleepycat.je.log;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.utilint.DbLsn;

/**
 * A class that implements LoggableObject can be stored as a JE log entry.
 */
public interface LoggableObject extends LogWritable {

    /**
     * All objects that are reponsible for a generating a type of log entry
     * must implement this.
     * @return the type of log entry 
     */
    public LogEntryType getLogType();

    /**
     * Do any processing we need to do after logging, while under the logging
     * latch.
     */
    public void postLogWork(DbLsn justLoggedLsn) 
        throws DatabaseException;

    /**
     * Return true if this item can be marshalled outside the log write
     * latch.
     */
    public boolean marshallOutsideWriteLatch();
}
