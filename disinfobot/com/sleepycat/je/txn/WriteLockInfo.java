/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: WriteLockInfo.java,v 1.1 2004/11/22 18:27:57 kate Exp $
*/

package com.sleepycat.je.txn;

import com.sleepycat.je.utilint.DbLsn;

/*
 * Lock and abort lsn kept for each write locked node. Allows us 
 * to log with the correct abort lsn.
 */
class WriteLockInfo {
    /* Write lock for node. */
    Lock lock;            

    /*
     * The original lsn. This is stored in the LN log entry.  May be
     * null if the node was created by this transaction.
     */
    DbLsn abortLsn;

    /*
     * The original setting of the knownDeleted flag.  It parallels
     * abortLsn.
     */
    boolean abortKnownDeleted;

    /*
     * True if the node has never been locked before. Used so we can
     * determine when to set abortLsn.
     */
    boolean neverLocked;

    /*
     * True if the node was created this transaction.
     */
    boolean createdThisTxn;

    WriteLockInfo(Lock lock) {
	this.lock = lock;
	abortLsn = DbLsn.NULL_LSN;
	abortKnownDeleted = false;
	neverLocked = true;
	createdThisTxn = false;
    }
}
