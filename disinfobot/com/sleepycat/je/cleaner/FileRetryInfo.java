/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: FileRetryInfo.java,v 1.1 2004/11/22 18:27:58 kate Exp $
*/

package com.sleepycat.je.cleaner;

import com.sleepycat.je.utilint.DbLsn;

/**
 * Cleaner retry information for a log file.
 *
 * <p>When retry information is maintained, a log file is divided into two
 * parts: the processed part and the unprocessed part.</p>
 *
 * <p>The processed part always starts at the beginning of the file and has
 * been fully cleaned except for pending entries.  Those entries are returned
 * by the pendingLsnIterator() method, and should be retried when the file is
 * processed again.</p>
 *
 * <p>The unprocessed part follows the processed part and starts with the LSN
 * returned by the getFirstUnprocessedLsn() method.  All entries in the log
 * from that LSN onward, including INs, should be cleaned when the file is
 * processed.  The unprocessed part of the file is present only if the
 * isFileFullyProcessed() method returns false.</p>
 *
 * <p>When there are no more pending entries in the processed part of the file
 * and the file is fully processed, the canFileBeDeleted() method will return
 * true and the file should be deleted.</p>
 */
interface FileRetryInfo {

    /**
     * Returns the log file number.
     */
    Long getFileNumber();

    /**
     * Ends processing of one cleaner cycle and indicates whether the file was
     * actually deleted by the cleaner.
     */
    void endProcessing(boolean deleted);

    /**
     * Returns whether all LSNs are obsolete.
     */
    boolean canFileBeDeleted();
    
    /**
     * Specifies that the file has been fully processed and only pending LNs
     * may prevent the file from being deleted.
     */
    void setFileFullyProcessed();

    /**
     * Returns true if all entries were previously processed, or false if more
     * entries should be processed starting with getFirstUnprocessedLsn().
     */
    boolean isFileFullyProcessed();

    /**
     * Specifies that all LSNs from the given LSN have not been processed.
     */
    void setFirstUnprocessedLsn(DbLsn lsn);

    /**
     * When isFileFullyProcessed() is false, returns the first LSN for
     * processing the file or null to start at the beginning of the file.
     */
    DbLsn getFirstUnprocessedLsn();

    /**
     * Returns an iterator of DbLsn objects collected in a prior cleaning
     * attempt when setPendingLN was called, or null if no LNs are pending.
     */
    DbLsn[] getPendingLsns();

    /**
     * Returns whether the given LN is known to be obsolete.
     */
    boolean isObsoleteLN(DbLsn lsn, long nodeId);

    /**
     * Changes the status of a given LN to obsolete.
     */
    void setObsoleteLN(DbLsn lsn, long nodeId);

    /**
     * Changes the status of a given LN to pending.
     */
    void setPendingLN(DbLsn lsn, long nodeId);
}
