/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: CmdUtil.java,v 1.1 2004/11/22 18:27:58 kate Exp $
*/

package com.sleepycat.je.utilint;

import java.io.File;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.EnvironmentImpl;

/**
 * Convenience methods for command line utilities
 */
public class CmdUtil {
    public static String getArg(String [] argv, int whichArg) 
        throws IllegalArgumentException {

        if (whichArg < argv.length) {
            return argv[whichArg];
        } else {
            throw new IllegalArgumentException();
        }
    }

    /**
     * Parse a string into a long. If the string starts with 0x, this is a 
     * hex number, else it's decimal.
     */
    public static long readLongNumber(String longVal) {
        if (longVal.startsWith("0x")) {
            return Long.parseLong(longVal.substring(2), 16);
        } else {
            return Long.parseLong(longVal);
        }
    }

    /**
     * Create an environment suitable for utilities. Utilities should in
     * general send trace output to the console and not to the db log.
     */
    public static EnvironmentImpl makeUtilityEnvironment(File envHome,
						  boolean readOnly)
        throws DatabaseException {
        
        EnvironmentConfig config = new EnvironmentConfig();
        config.setReadOnly(readOnly);
        
        // Don't debug log to the database log.
        config.setConfigParam(EnvironmentParams.JE_LOGGING_DBLOG.getName(),
			      "false");
        // Do debug log to the console
        config.setConfigParam(EnvironmentParams.JE_LOGGING_CONSOLE.getName(),
			      "true");

        // Set logging level to only show errors
        config.setConfigParam(EnvironmentParams.JE_LOGGING_LEVEL.getName(),
			      "SEVERE");

        // Don't run recovery.
        config.setConfigParam(EnvironmentParams.ENV_RECOVERY.getName(),
			      "false");

        return new EnvironmentImpl(envHome, config);
    }
}

