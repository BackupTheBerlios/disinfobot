/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: TriggerTransaction.java,v 1.1 2004/11/22 18:27:59 kate Exp $
*/

package com.sleepycat.je;

import com.sleepycat.je.txn.Txn;

/**
 * Prevents calling commit/abort from within a trigger.
 */
class TriggerTransaction extends Transaction {

    TriggerTransaction(Environment env, Txn txn) {
        super(env, txn, null);
    }

    public void abort()
	throws DatabaseException {

        throw notAllowed();
    }

    public void commit()
	throws DatabaseException {

        throw notAllowed();
    }

    public void commitSync()
	throws DatabaseException {

        throw notAllowed();
    }

    public void commitNoSync()
	throws DatabaseException {

        throw notAllowed();
    }

    private UnsupportedOperationException notAllowed() {

        return new UnsupportedOperationException(
                "Cannot call commit or abort within a trigger notification.");
    }
}
