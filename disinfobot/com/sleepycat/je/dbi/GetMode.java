/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: GetMode.java,v 1.1 2004/11/22 18:27:59 kate Exp $
*/

package com.sleepycat.je.dbi;

/**
 * Internal class used to distinguish which variety of getXXX()
 * that Cursor.retrieveNext should use.
 */
public class GetMode {
    private String name;

    private GetMode(String name) {
        this.name = name;
    }

    public static final GetMode NEXT =         new GetMode("NEXT");
    public static final GetMode PREV =         new GetMode("PREV");
    public static final GetMode NEXT_DUP =     new GetMode("NEXT_DUP");
    public static final GetMode PREV_DUP =     new GetMode("PREV_DUP");
    public static final GetMode NEXT_NODUP =   new GetMode("NEXT_NODUP");
    public static final GetMode PREV_NODUP =   new GetMode("PREV_NODUP");

    public String toString() {
        return name;
    }
}
