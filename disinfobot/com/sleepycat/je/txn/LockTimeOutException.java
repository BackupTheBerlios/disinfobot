/*
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: LockTimeOutException.java,v 1.1 2004/11/22 18:27:57 kate Exp $
*/

package com.sleepycat.je.txn;

import com.sleepycat.je.DeadlockException;

/**
 * A Lock timed out.
 */
public class LockTimeOutException extends DeadlockException {

    public LockTimeOutException() {
	super();
    }

    public LockTimeOutException(Throwable t) {
        super(t);
    }

    public LockTimeOutException(String message) {
	super(message);
    }

    public LockTimeOutException(String message, Throwable t) {
        super(message, t);
    }
}
