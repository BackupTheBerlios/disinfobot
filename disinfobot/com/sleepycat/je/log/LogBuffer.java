/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: LogBuffer.java,v 1.1 2004/11/22 18:27:58 kate Exp $
*/

package com.sleepycat.je.log;

import java.nio.ByteBuffer;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.latch.Latch;
import com.sleepycat.je.utilint.DbLsn;

/**
 * DbLogBuffers hold outgoing, newly written log entries.
 */
class LogBuffer implements LogSource {

    private static final String DEBUG_NAME = LogBuffer.class.getName();

    /* Storage */
    private ByteBuffer buffer;

    /* Information about what log entries are held here. */
    private DbLsn firstLsn;
    private DbLsn lastLsn;

    /* The read latch serializes access to and modification of the lsn info. */
    private Latch readLatch;

    LogBuffer(int capacity, EnvironmentImpl env)
	throws DatabaseException {

        if (env.useDirectNIO()) {
            buffer = ByteBuffer.allocateDirect(capacity);
        } else {
            buffer = ByteBuffer.allocate(capacity);
        }
        readLatch = new Latch(DEBUG_NAME, env);
        reinit();
    }

    void reinit()
	throws DatabaseException {

        readLatch.acquire();
        buffer.clear();
        firstLsn = null;
        lastLsn = null;
        readLatch.release();
    }

    /*
     * Write support
     */
        
    /**
     * Return first lsn held in this buffer. Assumes the log
     * write latch is held.
     */
    DbLsn getFirstLsn() {
        return firstLsn;
    }

    /**
     * This lsn has been written to the log.
     */
    void registerLsn(DbLsn lsn) throws DatabaseException {
        readLatch.acquire();
        if (lastLsn != null) {
            assert (lsn.compareTo(lastLsn)>0);
        }
        lastLsn = lsn;
        if (firstLsn == null) {
            firstLsn = lsn;
        }
        readLatch.release();
    }

    /**
     * Check capacity of buffer. Assumes that the log write latch is held.
     * @return true if this buffer can hold this many more bytes.
     */
    boolean hasRoom(int numBytes) {
        return (numBytes <= (buffer.capacity() - buffer.position()));
    }

    /**
     * @return the actual data buffer.
     */
    ByteBuffer getDataBuffer() {
        return buffer;
    }

    /**
     * @return capacity in bytes
     */
    int getCapacity() {
        return buffer.capacity();
    }

    /*
     * Read support
     */

    /**
     * Support for reading a log entry out of a still-in-memory log
     * @return true if this buffer holds the entry at this Lsn. The
     *         buffer will be latched for read. Returns false if 
     *         lsn is not here, and releases the read latch.
     */
    boolean containsLsn(DbLsn lsn)
	throws DatabaseException {

        /* Latch before we look at the LSNs. */
        readLatch.acquire();
        boolean found = false;
        if ((firstLsn != null) &&
            ((firstLsn.compareTo(lsn) <= 0) &&
	     (lastLsn.compareTo(lsn) >= 0))) {
            found = true;
        }
        
        if (found) {
            return true;
        } else {
            readLatch.release();
            return false;
        }
    }

    /*
     * LogSource support
     */

    /**
     * @see LogSource#release
     */
    public void release()
	throws DatabaseException  {

        if (readLatch.isOwner()) {
            readLatch.release();
        }
    }

    /**
     * @see LogSource#getBytes
     */
    public ByteBuffer getBytes(long fileOffset) {

        /*
         * Make a copy of this buffer (doesn't copy data, only buffer state)
         * and position it to read the requested data.
	 *
	 * Note that we catch Exception here because it is possible that
	 * another thread is modifying the state of buffer simultaneously.
	 * Specifically, this can happen if another thread is writing this log
	 * buffer out and it does (e.g.) a flip operation on it.  The actual
	 * mark/pos of the buffer may be caught in an unpredictable state.  We
	 * could add another latch to protect this buffer, but that's heavier
	 * weight than we need.  So the easiest thing to do is to just retry
	 * the duplicate operation.  See [#9822].
         */
        ByteBuffer copy = null;
	while (true) {
	    try {
		copy = buffer.duplicate();
		copy.position((int) (fileOffset - firstLsn.getFileOffset()));
		break;
	    } catch (IllegalArgumentException IAE) {
		continue;
	    }
	}
        return copy;
    }

    /**
     * @see LogSource#getBytes
     */
    public ByteBuffer getBytes(long fileOffset, int numBytes) {
        ByteBuffer copy = getBytes(fileOffset);
        /* Log Buffer should always hold a whole entry. */
        assert (copy.remaining() >= numBytes) :
            "copy.remaining=" + copy.remaining() +
            " numBytes=" + numBytes;
        return copy;
    }
}
