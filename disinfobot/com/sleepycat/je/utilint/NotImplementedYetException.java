/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: NotImplementedYetException.java,v 1.1 2004/11/22 18:27:58 kate Exp $
*/

package com.sleepycat.je.utilint;

/**
 * Something is not yet implemented.
 */
public class NotImplementedYetException extends RuntimeException {
    /**
     */
    public NotImplementedYetException() {
	super();
    }

    public NotImplementedYetException(String message) {
	super(message);
    }
}
