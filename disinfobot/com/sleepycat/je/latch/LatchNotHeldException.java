/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: LatchNotHeldException.java,v 1.1 2004/11/22 18:27:59 kate Exp $
*/

package com.sleepycat.je.latch;

/**
 * An exception that is thrown when a latch is not held but a method
 * is invoked on it that assumes it is held.
 */

public class LatchNotHeldException extends LatchException {
    public LatchNotHeldException() {
	super();
    }

    public LatchNotHeldException(String message) {
	super(message);
    }
}
