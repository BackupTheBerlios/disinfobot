/*
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: TxnTimeOutException.java,v 1.1 2004/11/22 18:27:57 kate Exp $
*/

package com.sleepycat.je.txn;

import com.sleepycat.je.DeadlockException;


/**
 * A Transaction timed out.
 */

public class TxnTimeOutException extends DeadlockException {

    public TxnTimeOutException() {
	super();
    }

    public TxnTimeOutException(Throwable t) {
        super(t);
    }

    public TxnTimeOutException(String message) {
	super(message);
    }

    public TxnTimeOutException(String message, Throwable t) {
        super(message, t);
    }
}
