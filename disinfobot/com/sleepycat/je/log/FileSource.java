/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: FileSource.java,v 1.1 2004/11/22 18:27:58 kate Exp $
*/

package com.sleepycat.je.log;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.EnvironmentImpl;

/**
 * FileSource is used as a channel to a log file when faulting in objects
 * from the log.
 */
class FileSource implements LogSource {

    private RandomAccessFile file;
    private int readBufferSize;

    FileSource(RandomAccessFile file,
	       int readBufferSize) {
        this.file = file;
        this.readBufferSize = readBufferSize;
    }

    /**
     * @see LogSource#release
     */
    public void release() 
        throws DatabaseException {
    }

    /**
     * @see LogSource#getBytes
     */
    public ByteBuffer getBytes(long fileOffset)
        throws IOException {
        
        // Fill up buffer from file
        ByteBuffer destBuf = ByteBuffer.allocate(readBufferSize);
        file.getChannel().read(destBuf, fileOffset);

	if (EnvironmentImpl.getForcedYield()) {
	    Thread.yield();
	}

        destBuf.flip();
        return destBuf;
    }

    /**
     * @see LogSource#getBytes
     */
    public ByteBuffer getBytes(long fileOffset, int numBytes)
        throws IOException {

        // Fill up buffer from file
        ByteBuffer destBuf = ByteBuffer.allocate(numBytes);
        file.getChannel().read(destBuf, fileOffset);

	if (EnvironmentImpl.getForcedYield()) {
	    Thread.yield();
	}

        destBuf.flip();
        
        assert destBuf.remaining() >= numBytes:
            "remaining=" + destBuf.remaining() +
            " numBytes=" + numBytes;
        return destBuf;
    }
}
