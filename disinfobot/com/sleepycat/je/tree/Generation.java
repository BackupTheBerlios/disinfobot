/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: Generation.java,v 1.1 2004/11/22 18:27:57 kate Exp $
*/

package com.sleepycat.je.tree;

public final class Generation {
    static private long nextGeneration = 0;

    static long getNextGeneration() {
	return nextGeneration++;
    }
}
