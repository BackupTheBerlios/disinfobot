/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: FileHandle.java,v 1.1 2004/11/22 18:27:58 kate Exp $
*/

package com.sleepycat.je.log;

import java.io.IOException;
import java.io.RandomAccessFile;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.latch.Latch;

/**
 * A FileHandle embodies a File and its accompanying latch.
 *
 * @version $Id: FileHandle.java,v 1.1 2004/11/22 18:27:58 kate Exp $
 */
class FileHandle {
    private RandomAccessFile file;
    private Latch fileLatch;

    FileHandle(RandomAccessFile file, String fileName, EnvironmentImpl env) {
        this.file = file;
        fileLatch = new Latch(fileName + "_fileHandle", env);
    }

    RandomAccessFile getFile() {
        return file;
    }

    void latch() 
        throws DatabaseException {

        fileLatch.acquire();
    }

    boolean latchNoWait() 
        throws DatabaseException {

        return fileLatch.acquireNoWait();
    }

    void release() 
        throws DatabaseException {

        fileLatch.release();
    }

    void close()
	throws IOException {

	if (file != null) {
	    file.close();
	    file = null;
	}
    }
}
