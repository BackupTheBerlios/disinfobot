/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: INLogEntry.java,v 1.1 2004/11/22 18:27:59 kate Exp $
*/

package com.sleepycat.je.log.entry;

import java.nio.ByteBuffer;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LoggableObject;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.utilint.DbLsn;

/**
 * INLogEntry embodies all IN log entries.  These entries contain an IN and a
 * databaseid. This class can both write out an entry and read one on.
 */
public class INLogEntry
    implements LogEntry, LoggableObject, INContainingEntry {

    /* Objects contained in an IN entry */
    private IN in;
    private DatabaseId dbId;
    
    private Class logClass;

    /**
     * Construct a log entry for reading.
     */
    public INLogEntry(Class logClass) {
        this.logClass = logClass;
    }

    /**
     * Construct a log entry for writing to the log.
     */
    public INLogEntry(IN in) {
        this.in = in;
        this.dbId = in.getDatabase().getId();
        this.logClass = in.getClass();
    }

    /*
     * Read support
     */

    /**
     * Read in an LN entry.
     */
    public void readEntry(ByteBuffer entryBuffer)
        throws DatabaseException {

        try {
            in = (IN) logClass.newInstance();
            in.readFromLog(entryBuffer);

            /* DatabaseImpl Id */
            dbId = new DatabaseId();
            dbId.readFromLog(entryBuffer);
        } catch (IllegalAccessException e) {
            throw new DatabaseException(e);
        } catch (InstantiationException e) {
            throw new DatabaseException(e);
        }
    }

    /**
     * Print out the contents of an entry.
     */
    public StringBuffer dumpEntry(StringBuffer sb, boolean verbose) {
        in.dumpLog(sb, verbose);
        dbId.dumpLog(sb, verbose);
        return sb;
    }

    /**
     * @return the item in the log entry
     */
    public Object getMainItem() {
        return in;
    }

    public Object clone()
        throws CloneNotSupportedException {

        return super.clone();
    }

    /**
     * @see LogEntry#isTransactional
     */
    public boolean isTransactional() {
	return false;
    }

    /**
     * @see LogEntry#getTransactionId
     */
    public long getTransactionId() {
	return 0;
    }

    /*
     * Writing support
     */

    /**
     * @see LoggableObject#getLogType
     */
    public LogEntryType getLogType() {
        return in.getLogType();
    }

    /**
     * @see LoggableObject#marshallOutsideWriteLatch
     * Ask the in if it can be marshalled outside the log write latch.
     */
    public boolean marshallOutsideWriteLatch() {
        return in.marshallOutsideWriteLatch();
    }

    /**
     * @see LoggableObject#postLogWork
     */
    public void postLogWork(DbLsn justLoggedLsn) {
    }

    /**
     * @see LoggableObject#getLogSize
     */
    public int getLogSize() {
        return (in.getLogSize() + dbId.getLogSize());
    }

    /**
     * @see LoggableObject#writeToLog
     */
    public void writeToLog(ByteBuffer destBuffer) {
        in.writeToLog(destBuffer);
        dbId.writeToLog(destBuffer);
    }

    /*
     * Access the in held within the entry.
     * @see INContainingEntry#getIN()
     */
    public IN getIN(EnvironmentImpl env)
        throws DatabaseException {
                
        return in;
    }

    /**
     * @see INContainingEntry#getDbId()
     */
    public DatabaseId getDbId() {

        return (DatabaseId) dbId;
    }

    /**
     * @return the lsn that represents this IN. For a vanilla IN entry, it's 
     * the last lsn read by the log reader.
     */
    public DbLsn getLsnOfIN(DbLsn lastReadLsn) {
        return lastReadLsn;
    }
}
