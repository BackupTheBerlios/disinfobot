/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2000-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: TransactionStatsInternal.java,v 1.1 2004/11/22 18:27:57 kate Exp $
*/

package com.sleepycat.je.txn;

import com.sleepycat.je.TransactionStats;
import com.sleepycat.je.utilint.DbLsn;

/**
 * Javadoc for this public class is generated
 * via the doc templates in the doc_src directory.
 */
public class TransactionStatsInternal extends TransactionStats {

    /**
     * The DbLsn of the last checkpoint.
     */
    private DbLsn lastCheckpointLsn;


    /**
     */
    public DbLsn getLastCheckpointLsn() {
        return lastCheckpointLsn;
    }

    /**
     * @param lsn
     */
    public void setLastCheckpointLsn(DbLsn lsn) {
        lastCheckpointLsn = lsn;
    }
}
