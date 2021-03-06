/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: RecoveryException.java,v 1.1 2004/11/22 18:27:59 kate Exp $
*/

package com.sleepycat.je.recovery;

import com.sleepycat.je.RunRecoveryException;
import com.sleepycat.je.dbi.EnvironmentImpl;

/**
 * Recovery related exceptions
 */

public class RecoveryException extends RunRecoveryException {
    public RecoveryException(EnvironmentImpl env,
                             String message,
                             Throwable t) {
	super(env, message, t);
    }
    public RecoveryException(EnvironmentImpl env,
                             String message) {
	super(env, message);
    }
}
