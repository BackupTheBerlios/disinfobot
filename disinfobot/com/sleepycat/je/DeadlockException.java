/*
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: DeadlockException.java,v 1.1 2004/11/22 18:27:58 kate Exp $
*/

package com.sleepycat.je;

/**
 * Javadoc for this public class is generated
 * via the doc templates in the doc_src directory.
 */
public class DeadlockException extends DatabaseException {

    public DeadlockException() {
	super();
    }

    public DeadlockException(Throwable t) {
        super(t);
    }

    public DeadlockException(String message) {
	super(message);
    }

    public DeadlockException(String message, Throwable t) {
        super(message, t);
    }
}
