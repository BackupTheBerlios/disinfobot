/*
 * See the file LICENSE for redistribution iormation.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: BINDeltaLogEntry.java,v 1.1 2004/11/22 18:27:59 kate Exp $ 
*/
package com.sleepycat.je.log.entry;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.tree.BINDelta;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.utilint.DbLsn;

/**
 * A BINDeltaLogEntry knows how to create a whole BIN from a delta entry.
 */
public class BINDeltaLogEntry extends SingleItemLogEntry
    implements INContainingEntry {

    /**
     * @param logClass
     */
    public BINDeltaLogEntry(Class logClass) {
        super(logClass);
    }

    /* 
     * @see com.sleepycat.je.log.entry.INContainingEntry#getIN()
     */
    public IN getIN(EnvironmentImpl env) 
    	throws DatabaseException {

        BINDelta delta = (BINDelta) getMainItem();
        return delta.reconstituteBIN(env);
    }

    /*
     * @see com.sleepycat.je.log.entry.INContainingEntry#getDbId()
     */
    public DatabaseId getDbId() {
        BINDelta delta = (BINDelta) getMainItem();
        return delta.getDbId();	
    }

    /**
     * @return the lsn that represents this IN. For this BINDelta, it's
     * the last full version.
     */
    public DbLsn getLsnOfIN(DbLsn lastReadLsn) {
        BINDelta delta = (BINDelta) getMainItem();
        return delta.getLastFullLsn();
    }
}
