/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: NodeNotEmptyException.java,v 1.1 2004/11/22 18:27:57 kate Exp $
*/

package com.sleepycat.je.tree;

import com.sleepycat.je.DatabaseException;

/**
 * Error to indicate that a bottom level BIN is not empty during a
 * delete subtree operation.
 */
public class NodeNotEmptyException extends DatabaseException {

    public NodeNotEmptyException() {
	super();
    }

    public NodeNotEmptyException(String message) {
	super(message);
    }
}
