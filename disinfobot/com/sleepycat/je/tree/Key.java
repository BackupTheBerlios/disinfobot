/*-
* See the file LICENSE for redistribution inyformation.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: Key.java,v 1.1 2004/11/22 18:27:57 kate Exp $
*/

package com.sleepycat.je.tree;

import java.nio.ByteBuffer;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.log.LogReadable;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.log.LogWritable;

/**
 * Key represents a JE B-Tree Key.  Keys are immutable.
 */
public final class Key implements Comparable, LogWritable, LogReadable {
    public static boolean DUMP_BINARY = false;
    private byte[] key;

    /**
     * Construct a new key.
     */
    public Key() {
	this.key = null;
    }

    /**
     * Construct a new key from a byte array.
     */
    public Key(byte[] key) {
	if (key == null) {
	    this.key = null;
	} else {
            init(key, 0, key.length);
	}
    }

    /**
     * Construct a new key from a DatabaseEntry.
     */
    public Key(DatabaseEntry dbt) {
        byte[] key = dbt.getData();
	if (key == null) {
	    this.key = null;
	} else {
            init(key, dbt.getOffset(), dbt.getSize());
	}
    }

    private void init(byte[] key, int off, int len) {
        this.key = new byte[len];
        System.arraycopy(key, off, this.key, 0, len);
    }

    /**
     * Get the byte array for the key.
     */
    public byte[] getKey() {
	return key;
    }

    public byte[] copy() {
	int len = key.length;
	byte[] ret = new byte[len];
	System.arraycopy(key, 0, ret, 0, len);
	return ret;
    }

    /**
     * Compare two keys.  Standard compareTo function and returns.
     */
    public int compareTo(Object o) {
	if (o == null) {
	    throw new NullPointerException();
	}

        Key argKey = (Key) o;
        return compareByteArray(this.key, argKey.key);
    }

    /**
     * Support Set of Key in BINReference.
     */
    public boolean equals(Object o) {
        return (o instanceof Key) && (compareTo(o) == 0);
    }

    /**
     * Support HashSet of Key in BINReference.
     */
    public int hashCode() {
        int code = 0;
        for (int i = 0; i < key.length; i += 1) {
            code += key[i];
        }
        return code;
    }

    static public int compareByteArray(byte[] arg1, byte[] arg2) {
	int a1Len = arg1.length;
	int a2Len = arg2.length;

	int limit = Math.min(a1Len, a2Len);

	for (int i = 0; i < limit; i++) {
	    byte b1 = arg1[i];
	    byte b2 = arg2[i];
	    if (b1 == b2) {
		continue;
	    } else {
		/* Remember, bytes are signed, so convert to shorts so that
		   we effectively do an unsigned byte comparison. */
		return (b1 & 0xff) - (b2 & 0xff);
	    }
	}

	return (a1Len - a2Len);
    }

    public String toString() {
	StringBuffer sb = new StringBuffer();
	sb.append("<key v=\"");

        /** uncomment for hex formatting 
	    for (int i = 0 ; i < key.length; i++) {
	    sb.append(Integer.toHexString(key[i] & 0xFF)).append(" ");
	    }
        **/

	if (DUMP_BINARY) {
	    if (key != null) {
		sb.append(TreeUtils.dumpByteArray(key));
	    } else {
		sb.append("<null>");
	    }
	} else {
	    sb.append(key == null ? "" : new String(key));
	}
	sb.append("\"/>");

	return sb.toString();
    }

    /**
     * Print the string w/out XML format.
     */
    public String getNoFormatString() {
        return "key=" + key;
    }

    String dumpString(int nspaces){
        return TreeUtils.indent(nspaces) + toString();
    }

    /*
     * Logging support
     */

    /**
     * @see LogWritable#getLogSize
     */
    public int getLogSize() {
        return LogUtils.getByteArrayLogSize(key);
    }

    /**
     * @see LogWritable#writeToLog
     */
    public void writeToLog(ByteBuffer logBuffer) {
        LogUtils.writeByteArray(logBuffer, key);
    }

    /**
     * @see LogReadable#readFromLog
     */
    public void readFromLog(ByteBuffer itemBuf) {
        key = LogUtils.readByteArray(itemBuf);
    }

    /**
     * @see LogReadable#dumpLog
     */
    public void dumpLog(StringBuffer sb, boolean verbose) {
        sb.append("<key val=\"");
	if (DUMP_BINARY) {
            sb.append(TreeUtils.dumpByteArray(key));
	} else {
	    sb.append(key == null ? "" : new String(key));
	}
        sb.append("\"/>");
    }  

    /**
     * @see LogReadable#logEntryIsTransactional
     */
    public boolean logEntryIsTransactional() {
	return false;
    }

    /**
     * @see LogReadable#getTransactionId
     */
    public long getTransactionId() {
	return 0;
    }
}
