/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: SecondaryTrigger.java,v 1.1 2004/11/22 18:27:59 kate Exp $
*/

package com.sleepycat.je;

class SecondaryTrigger implements DatabaseTrigger {

    private SecondaryDatabase secDb;

    SecondaryTrigger(SecondaryDatabase secDb) {

        this.secDb = secDb;
    }

    final SecondaryDatabase getDb() {

        return secDb;
    }

    public void triggerAdded(Database db) {
    }

    public void triggerRemoved(Database db) {

        secDb.clearPrimary();
    }

    public void databaseUpdated(Database db,
                                Transaction txn,
                                DatabaseEntry priKey,
                                DatabaseEntry oldData,
                                DatabaseEntry newData)
        throws DatabaseException {

        secDb.updateSecondary((txn != null) ? txn.getLocker() : null, null,
                              priKey, oldData, newData);
    }
}
