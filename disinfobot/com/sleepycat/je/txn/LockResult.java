/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: LockResult.java,v 1.1 2004/11/22 18:27:57 kate Exp $
*/

package com.sleepycat.je.txn;

import com.sleepycat.je.utilint.DbLsn;

/**
 * This class is a container to encapsulate a LockGrantType and a
 * WriteLockInfo so that they can both be returned from writeLock.
 */
public class LockResult {
    private LockGrantType grant;
    private WriteLockInfo info;

    /* made public for unittests */
    public LockResult(LockGrantType grant, WriteLockInfo info) {
	this.grant = grant;
	this.info = info;
    }

    public LockGrantType getLockGrant() {
	return grant;
    }

    public void setAbortLsn(DbLsn abortLsn, boolean abortKnownDeleted) {
	setAbortLsnInternal(abortLsn, abortKnownDeleted, false);
    }

    public void setAbortLsn(DbLsn abortLsn,
			    boolean abortKnownDeleted,
			    boolean createdThisTxn) {
	setAbortLsnInternal(abortLsn, abortKnownDeleted, createdThisTxn);
    }

    private void setAbortLsnInternal(DbLsn abortLsn,
				     boolean abortKnownDeleted,
				     boolean createdThisTxn) {
	/* info can be null if this is called on behalf of a BasicLocker. */
	if (info != null &&
	    info.neverLocked) {
	    /* Only set if not null, otherwise keep NULL_LSN as abortLsn. */
	    if (abortLsn != null) {
		info.abortLsn = abortLsn;
		info.abortKnownDeleted = abortKnownDeleted;
	    }
	    info.createdThisTxn = createdThisTxn;
	    info.neverLocked = false;
	}
    }
}
