/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: ForeignKeyTrigger.java,v 1.1 2004/11/22 18:27:59 kate Exp $
*/

package com.sleepycat.je;

class ForeignKeyTrigger implements DatabaseTrigger {

    private SecondaryDatabase secDb;

    ForeignKeyTrigger(SecondaryDatabase secDb) {

        this.secDb = secDb;
    }

    public void triggerAdded(Database db) {
    }

    public void triggerRemoved(Database db) {

        secDb.clearForeignKeyTrigger();
    }

    public void databaseUpdated(Database db,
                                Transaction txn,
                                DatabaseEntry priKey,
                                DatabaseEntry oldData,
                                DatabaseEntry newData)
        throws DatabaseException {

        if (newData == null) {
            secDb.onForeignKeyDelete((txn != null) ? txn.getLocker() : null, 
                                      priKey);
        }
    }
}
