/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: TreeLocation.java,v 1.1 2004/11/22 18:27:57 kate Exp $
*/

package com.sleepycat.je.tree;

import com.sleepycat.je.utilint.DbLsn;

/*
 * TreeLocation is a cursor like object that keeps track of a location
 * in a tree. It's used during recovery.
 */
public class TreeLocation {
    public BIN bin;         // parent BIN for the target LN
    public int index;       // index of where the LN is or should go
    public Key lnKey;       // the key that represents this LN in this BIN
    public DbLsn childLsn;  // current lsn value in that slot.

    public void reset() {
        bin = null;
        index = -1;
        lnKey = null;
        childLsn = null;
    }

    public String toString() {
	StringBuffer sb = new StringBuffer("<TreeLocation bin=\"");
	if (bin == null) {
	    sb.append("null");
	} else {
	    sb.append(bin.getNodeId());
	}
	sb.append("\" index=\"");
	sb.append(index);
	sb.append("\" lnKey=\"");
	sb.append(lnKey);
	sb.append("\" childLsn=\"");
	sb.append(childLsn);
	sb.append("\">");
	return sb.toString();
    }
}

