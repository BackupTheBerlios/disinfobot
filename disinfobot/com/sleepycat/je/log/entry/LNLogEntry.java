/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: LNLogEntry.java,v 1.1 2004/11/22 18:27:59 kate Exp $
*/

package com.sleepycat.je.log.entry;

import java.nio.ByteBuffer;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LoggableObject;
import com.sleepycat.je.tree.Key;
import com.sleepycat.je.tree.LN;
import com.sleepycat.je.txn.Txn;
import com.sleepycat.je.utilint.DbLsn;

/**
 * LNLogEntry embodies all LN transactional log entries.
 * These entries contain:
 * <pre>
 *   ln
 *   databaseid
 *   key            
 *   abortLsn       -- if transactional
 *   abortKnownDeleted -- if transactional
 *   txn            -- if transactional
 * </pre>
 */
public class LNLogEntry implements LogEntry, LoggableObject {

    /* Objects contained in an LN entry */
    private LN ln;
    private DatabaseId dbId;
    private Key key;
    private DbLsn abortLsn;
    private boolean abortKnownDeleted;
    private Txn txn;
    
    private static final byte ABORT_KNOWN_DELETED_MASK = (byte) 1;

    /* Class used to instantiate the main item in this entry */
    private Class logClass;           // used for reading a log entry
    private LogEntryType entryType; // used for writing a log entry

    /* 
     * Note: used this flag instead of splitting this class into a txnal and
     * non-txnal version because we want to subclass it with the duplicate
     * deleted entry, which also comes in txnal and non-txnal form.
     */
    private boolean isTransactional; 

    /* Constructor to read an entry. */
    public LNLogEntry(Class logClass, boolean isTransactional) {
        this.logClass = logClass;
        this.isTransactional = isTransactional;
    }

    /* Constructor to write an entry. */
    public LNLogEntry(LogEntryType entryType,
                      LN ln,
		      DatabaseId dbId,
		      Key key,
                      DbLsn abortLsn,
		      boolean abortKnownDeleted,
		      Txn txn) {
        this.entryType = entryType;
        this.ln = ln;
        this.dbId = dbId;
        this.key = key;
        this.abortLsn = abortLsn;
	this.abortKnownDeleted = abortKnownDeleted;
        this.txn = txn;
        this.isTransactional = (txn != null);
        this.logClass = ln.getClass();
    }

    /**
     * @see LogEntry#readEntry
     */
    public void readEntry(ByteBuffer entryBuffer)
        throws DatabaseException {

        try {
            /* LN */
            ln = (LN) logClass.newInstance();
            ln.readFromLog(entryBuffer);

            /* DatabaseImpl Id */
            dbId = new DatabaseId();
            dbId.readFromLog(entryBuffer);

            /* Key */
            key = new Key();
            key.readFromLog(entryBuffer);

            if (isTransactional) {

                /*
                 * AbortLsn. If it was a marker lsn that was used to fill in a
                 * create, mark it null.
                 */
                abortLsn = new DbLsn();
                abortLsn.readFromLog(entryBuffer);
                if (abortLsn.getFileNumber() ==
		    DbLsn.NULL_LSN.getFileNumber()) {
                    abortLsn = null;
                }

		abortKnownDeleted =
		    ((entryBuffer.get() & ABORT_KNOWN_DELETED_MASK) != 0) ?
		    true : false;

                /* Locker */
                txn = new Txn();
                txn.readFromLog(entryBuffer);
            }
        } catch (IllegalAccessException e) {
            throw new DatabaseException(e);
        } catch (InstantiationException e) {
            throw new DatabaseException(e);
        }
    }

    /**
     * @see LogEntry#dumpEntry
     */
    public StringBuffer dumpEntry(StringBuffer sb, boolean verbose) {
        ln.dumpLog(sb, verbose);
        dbId.dumpLog(sb, verbose);
        key.dumpLog(sb, verbose);
        if (isTransactional) {
            if (abortLsn != null) {
                abortLsn.dumpLog(sb, verbose);
            }
	    sb.append("<knownDeleted val=\"");
	    sb.append(abortKnownDeleted ? "true" : "false");
	    sb.append("\"/>");
            txn.dumpLog(sb, verbose);
        }
        return sb;
    }

    /**
     * @see LogEntry#getMainItem
     */
    public Object getMainItem() {
        return ln;
    }

    /**
     * @see LogEntry#clone
     */
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    /**
     * @see LogEntry#isTransactional
     */
    public boolean isTransactional() {
	return isTransactional;
    }

    /**
     * @see LogEntry#getTransactionId
     */
    public long getTransactionId() {
	if (isTransactional) {
	    return txn.getId();
	} else {
	    return 0;
	}
    }

    /*
     * Writing support
     */

    /**
     * @see LoggableObject#getLogType
     */
    public LogEntryType getLogType() {
        return entryType;
    }

    /**
     * @see LoggableObject#marshallOutsideWriteLatch
     * Ask the ln if it can be marshalled outside the log write latch.
     */
    public boolean marshallOutsideWriteLatch() {
        return ln.marshallOutsideWriteLatch();
    }

    /**
     * For LN entries, we need to record the latest lsn for that node with the
     * owning transaction, within the protection of the log latch. This is a
     * callback for the log manager to do that recording.
     *
     * @see LoggableObject#postLogWork
     */
    public void postLogWork(DbLsn justLoggedLsn)
        throws DatabaseException {

        if (isTransactional) {
            txn.addLogInfo(ln.getNodeId(), justLoggedLsn);
        }
    }

    /**
     * @see LoggableObject#getLogSize
     */
    public int getLogSize(){
        int size = ln.getLogSize() +
            dbId.getLogSize() +
            key.getLogSize();
        if (isTransactional) {
            if (abortLsn != null) {
                size += abortLsn.getLogSize();
            }
	    size++;   // abortKnownDeleted
            size += txn.getLogSize();
        }
        return size;
    }

    /**
     * @see LoggableObject#writeToLog
     */
    public void writeToLog(ByteBuffer destBuffer) {
        ln.writeToLog(destBuffer);
        dbId.writeToLog(destBuffer);
        key.writeToLog(destBuffer);
        if (isTransactional) { 
            if (abortLsn == null) {
                DbLsn.NULL_LSN.writeToLog(destBuffer);
            } else {
                abortLsn.writeToLog(destBuffer);
            }
	    byte aKD = 0;
	    if (abortKnownDeleted) {
		aKD |= ABORT_KNOWN_DELETED_MASK;
	    }
	    destBuffer.put(aKD);
            txn.writeToLog(destBuffer);
        }
    }

    /*
     * Accessors
     */
    public LN getLN() {
        return ln;
    }

    public DatabaseId getDbId() {
        return dbId;
    }

    public Key getKey() {
        return key;
    }

    public Key getDupKey() {
        if (ln.isDeleted()) {
            return null;
        } else {
            return new Key(ln.getData());
        }
    }

    public DbLsn getAbortLsn() {
        return abortLsn;
    }

    public boolean getAbortKnownDeleted() {
	return abortKnownDeleted;
    }

    public Long getTxnId() {
        if (isTransactional) {
            return new Long(txn.getId());
        } else {
            return null;
        }
    }

    public Txn getUserTxn() {
        if (isTransactional) {
            return txn;
        } else {
            return null;
        }
    }
}
