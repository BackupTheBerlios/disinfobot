/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: TraceLogHandler.java,v 1.1 2004/11/22 18:27:58 kate Exp $
*/

package com.sleepycat.je.log;

import java.util.logging.Handler;
import java.util.logging.LogRecord;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.utilint.Tracer;

/**
 * Handler for java.util.logging. Takes logging records and publishes them
 * into the database log.
 */
public class TraceLogHandler extends Handler {

    private EnvironmentImpl env;

    public TraceLogHandler(EnvironmentImpl env) {
        this.env = env;
    }

    public void close() {
    }

    public void flush() {
    }

    public void publish(LogRecord l) {
        if (!env.isReadOnly()) {
            try {
                Tracer newRec = new Tracer(l.getMessage());
                env.getLogManager().log(newRec);
            } catch (DatabaseException e) {
                // eat exception
                System.out.println("Problem seen while tracing into " +
                                   "the database log:");
                e.printStackTrace();
                assert false : 
                    "Couldn't log debug records, should shut off this handler";
            }
        }
    }
}
