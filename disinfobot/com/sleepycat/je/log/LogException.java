/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: LogException.java,v 1.1 2004/11/22 18:27:58 kate Exp $
*/

package com.sleepycat.je.log;

import com.sleepycat.je.DatabaseException;

/**
 * Configuration related exceptions
 */
public class LogException extends DatabaseException {
    public LogException(String message) {
	super(message);
    }

    public LogException(String message, Exception e) {
	super(message, e);
    }
}

