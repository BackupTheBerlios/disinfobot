/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: LogFileNotFoundException.java,v 1.1 2004/11/22 18:27:58 kate Exp $
*/

package com.sleepycat.je.log;


/**
 * Log file doesn't exist.
 */
public class LogFileNotFoundException extends LogException {
    public LogFileNotFoundException(String message) {
	super(message);
    }
}

