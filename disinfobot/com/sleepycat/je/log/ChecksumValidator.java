/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: ChecksumValidator.java,v 1.1 2004/11/22 18:27:58 kate Exp $
*/

package com.sleepycat.je.log;

import java.nio.ByteBuffer;
import java.util.zip.Checksum;

import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.utilint.Adler32;
import com.sleepycat.je.utilint.DbLsn;

/**
 * Checksum validator is used to check checksums on log entries.
 */
class ChecksumValidator {
    private static final boolean DEBUG = false;

    private Checksum cksum;

    ChecksumValidator() {
        cksum = new Adler32();
    }

    void reset() {
        cksum.reset();
    }

    /**
     * Add this byte buffer to the checksum. Assume the byte buffer is already
     * positioned at the data.
     * @param buf target buffer
     * @param length of data
     */
    void update(EnvironmentImpl env, ByteBuffer buf, int length)
        throws DbChecksumException {

        if (buf == null) {
            throw new DbChecksumException
		(env, "null buffer given to checksum validation, probably " +
		 " result of 0's in log file.");
        }

        int bufStart = buf.position();

        if (DEBUG) {
            System.out.println("bufStart = " + bufStart +
                               " length = " + length);
        }

        if (buf.hasArray()) {
            cksum.update(buf.array(), bufStart, length);
        } else {
            for (int i = bufStart; i < (length + bufStart); i++) {
                cksum.update(buf.get(i));
            }
        }
    }

    void validate(EnvironmentImpl env,
                  long expectedChecksum,
                  DbLsn lsn)
        throws DbChecksumException {

        if (expectedChecksum != cksum.getValue()) {
            throw new DbChecksumException
		(env, "Location " + lsn.getNoFormatString() + " expected " +
		 expectedChecksum + " got " + cksum.getValue());
        }
    }

    void validate(EnvironmentImpl env,
                  long expectedChecksum,
                  long fileNum,
                  long fileOffset)
        throws DbChecksumException {

        if (expectedChecksum != cksum.getValue()) {
            DbLsn problemLsn = new DbLsn(fileNum, fileOffset);
            throw new DbChecksumException
		(env, "Location " + problemLsn.getNoFormatString() +
		 " expected " + expectedChecksum + " got " +
		 cksum.getValue());
        }
    }
}
