/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: LatchException.java,v 1.1 2004/11/22 18:27:59 kate Exp $
*/

package com.sleepycat.je.latch;

import com.sleepycat.je.DatabaseException;

/**
 * The root of latch related exceptions.
 */

public class LatchException extends DatabaseException {

    public LatchException() {
	super();
    }

    public LatchException(String message) {
	super(message);
    }
}
