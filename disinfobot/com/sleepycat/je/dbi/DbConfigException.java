/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: DbConfigException.java,v 1.1 2004/11/22 18:27:59 kate Exp $
*/

package com.sleepycat.je.dbi;

import com.sleepycat.je.DatabaseException;

/**
 * Configuration related exceptions
 */
public class DbConfigException extends DatabaseException {

    public DbConfigException(Throwable t) {
        super(t);
    }

    public DbConfigException(String message) {
	super(message);
    }

    public DbConfigException(String message, Throwable t) {
        super(message, t);
    }
}
