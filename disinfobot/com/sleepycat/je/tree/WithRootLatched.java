/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: WithRootLatched.java,v 1.1 2004/11/22 18:27:57 kate Exp $
*/

package com.sleepycat.je.tree;

import com.sleepycat.je.DatabaseException;

public interface WithRootLatched {
    
    /**
     * doWork is called while the tree's root latch is held.
     */
    public IN doWork(ChildReference root)
	throws DatabaseException;
}
