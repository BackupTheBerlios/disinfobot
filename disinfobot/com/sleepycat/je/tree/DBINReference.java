/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: DBINReference.java,v 1.1 2004/11/22 18:27:57 kate Exp $
*/

package com.sleepycat.je.tree;

import com.sleepycat.je.dbi.DatabaseId;

/**
 * A class that embodies a reference to a DBIN that does not rely on a
 * java reference to the actual DBIN.
 */
public class DBINReference extends BINReference {
    private Key dupKey;

    DBINReference(long nodeId, DatabaseId databaseId, Key idKey, Key dupKey) {
	super(nodeId, databaseId, idKey);
	this.dupKey = dupKey;
    }

    public Key getKey() {
	return dupKey;
    }

    public Key getData() {
	return idKey;
    }
}

