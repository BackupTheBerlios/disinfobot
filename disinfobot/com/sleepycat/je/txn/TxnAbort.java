/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: TxnAbort.java,v 1.1 2004/11/22 18:27:57 kate Exp $
*/

package com.sleepycat.je.txn;

import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.utilint.DbLsn;

/**
 * This class writes out a transaction commit or transaction end record.
 */
public class TxnAbort extends TxnEnd {
    public TxnAbort(long id, DbLsn lastLsn) {
        super(id, lastLsn);
    }
    
    /**
     * For constructing from the log.
     */
    public TxnAbort() {
    }

    /*
     * Log support
     */

    /**
     * @see TxnEnd#getLogType
     */
    public LogEntryType getLogType() {
        return LogEntryType.LOG_TXN_ABORT;
    }

    protected String getTagName() {
        return "TxnAbort";
    }
}
