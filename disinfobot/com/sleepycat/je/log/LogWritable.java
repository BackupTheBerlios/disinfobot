/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: LogWritable.java,v 1.1 2004/11/22 18:27:58 kate Exp $
*/

package com.sleepycat.je.log;

import java.nio.ByteBuffer;

/**
 * A class that implements LogWritable knows how to write itself into
 * the JE log.
 */
public interface LogWritable {

    /**
     * @return number of bytes used to store this object
     */
    public int getLogSize();

    /**
     * Serialize this object into the buffer. 
     * @param logBuffer is the destination buffer
     */
    public void writeToLog(ByteBuffer logBuffer);
}
