/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: DeletedDupLNLogEntry.java,v 1.1 2004/11/22 18:27:59 kate Exp $
*/

package com.sleepycat.je.log.entry;

import java.nio.ByteBuffer;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.tree.Key;
import com.sleepycat.je.tree.LN;
import com.sleepycat.je.txn.Txn;
import com.sleepycat.je.utilint.DbLsn;

/**
 * DupDeletedLNEntry encapsulates a deleted dupe ln entry. This contains all
 * the regular transactional LN log entry fields and an extra key, which is 
 * nulled out data field of the LN (which becomes the key in the duplicate
 * tree.
 */
public class DeletedDupLNLogEntry extends LNLogEntry {

    /* 
     * Deleted duplicate LN must log an entra key in their log entries,
     * because the data field that is the "key" in a dup tree has been
     * nulled out because the LN is deleted.
     */
    private Key dataAsKey;

    /**
     * Constructor to read an entry.
     */
    public DeletedDupLNLogEntry(boolean isTransactional) {
        super(com.sleepycat.je.tree.LN.class, isTransactional);
    }

    /**
     * Constructor to make an object that can write this entry.
     */
    public DeletedDupLNLogEntry(LogEntryType entryType,
                                LN ln,
                                DatabaseId dbId,
                                Key key,
                                Key dataAsKey,
                                DbLsn abortLsn,
                                boolean abortKnownDeleted,
                                Txn txn) {
        super(entryType, ln, dbId, key, abortLsn, abortKnownDeleted, txn);
        this.dataAsKey = dataAsKey;
    }

    /**
     * Extends its super class to read in the extra dup key.
     * @see LNLogEntry#readEntry
     */
    public void readEntry(ByteBuffer entryBuffer)
        throws DatabaseException {
        super.readEntry(entryBuffer);

        /* Key */
        dataAsKey = new Key();
        dataAsKey.readFromLog(entryBuffer);
    }

    /**
     * Extends super class to dump out extra key.
     * @see LNLogEntry#dumpEntry
     */
    public StringBuffer dumpEntry(StringBuffer sb, boolean verbose) {
        super.dumpEntry(sb, verbose);
        dataAsKey.dumpLog(sb, verbose);
        return sb;
    }
    
    /*
     * Writing support
     */

    /**
     * Extend super class to add in extra key.
     * @see LNLogEntry#getLogSize
     */
    public int getLogSize(){
        return super.getLogSize() + dataAsKey.getLogSize();
    }

    /**
     * @see LNLogEntry#writeToLog
     */
    public void writeToLog(ByteBuffer destBuffer) {
        super.writeToLog(destBuffer);
        dataAsKey.writeToLog(destBuffer);
    }

    /*
     * Accessors
     */

    /**
     * Get the data-as-key out of the entry.
     */
    public Key getDupKey() {
        return dataAsKey;
    }
}
