/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: LockGrantType.java,v 1.1 2004/11/22 18:27:57 kate Exp $
*/

package com.sleepycat.je.txn;

/**
 * LockGrantType is an enumeration of the possible results of a lock
 * attempt.
 */
public class LockGrantType {
    private String name;

    /* Grant types */
    public static final LockGrantType NEW =
	new LockGrantType("NEW");
    public static final LockGrantType WAIT_NEW =
	new LockGrantType("WAIT_NEW");
    public static final LockGrantType PROMOTION =
	new LockGrantType("PROMOTION");
    public static final LockGrantType WAIT_PROMOTION =
	new LockGrantType("WAIT_PROMOTION");
    public static final LockGrantType EXISTING =
	new LockGrantType("EXISTING");
    public static final LockGrantType DENIED =
	new LockGrantType("DENIED");
    public static final LockGrantType NONE_NEEDED =
	new LockGrantType("NONE_NEEDED");

    /* No lock types can be defined outside this class */
    private LockGrantType(String name) {
        this.name = name;
    }

    public String toString() {
        return name;
    }
}
