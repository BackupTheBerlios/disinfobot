/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: DuplicateEntryException.java,v 1.1 2004/11/22 18:27:57 kate Exp $
*/

package com.sleepycat.je.tree;

import com.sleepycat.je.DatabaseException;

/**
 * Exception to indicate that an entry is already present in a node.
 */
public class DuplicateEntryException extends DatabaseException {
    public DuplicateEntryException() {
	super();
    }

    public DuplicateEntryException(String message) {
	super(message);
    }
}
