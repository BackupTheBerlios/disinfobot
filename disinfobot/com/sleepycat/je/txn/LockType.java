/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: LockType.java,v 1.1 2004/11/22 18:27:57 kate Exp $
*/

package com.sleepycat.je.txn;

/**
 * LockType is a type safe enumeration of all lock types.
 */
public class LockType {
    /* typeNumber is the persistent value for this lock type. */
    private short typeNumber;

    /* Node types */
    public static final LockType READ       = new LockType((short) 1);
    public static final LockType WRITE      = new LockType((short) 2);

    /* No lock types can be defined outside this class */
    private LockType(short typeNum) {
        typeNumber = typeNum;
    }

    /* Only classes in this package need know about the type number */
    short getTypeNumber() {
        return typeNumber;
    }

    public String toString() {
	if (this == READ) {
	    return "READ";
	} else if (this == WRITE) {
	    return "WRITE";
	} else {
	    return "unknown: " + typeNumber;
	}
    }
}
