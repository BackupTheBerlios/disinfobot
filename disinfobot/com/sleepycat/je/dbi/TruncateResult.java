/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: TruncateResult.java,v 1.1 2004/11/22 18:27:59 kate Exp $
*/

package com.sleepycat.je.dbi;

/**
 * Holds the result of a database truncate operation.
 */
public class TruncateResult {

    private DatabaseImpl db;
    private int count;

    TruncateResult(DatabaseImpl db, int count) {
        this.db = db;
        this.count = count;
    }

    public DatabaseImpl getDatabase() {
        return db;
    }

    public int getRecordCount() {
        return count;
    }
}
