/*
 * See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: INContainingEntry.java,v 1.1 2004/11/22 18:27:59 kate Exp $ 
*/
package com.sleepycat.je.log.entry;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.utilint.DbLsn;

/**
 * An INContainingEntry is a log entry that contains internal nodes.
 */
public interface INContainingEntry {
	
    /**
     * @return the IN held within this log entry.
     */
    public IN getIN(EnvironmentImpl env) 
        throws DatabaseException;
	
    /**
     * @return the database id held within this log entry.
     */
    public DatabaseId getDbId();

    /**
     * @return the lsn that represents this IN.
     */
    public DbLsn getLsnOfIN(DbLsn lastReadLsn);
}
