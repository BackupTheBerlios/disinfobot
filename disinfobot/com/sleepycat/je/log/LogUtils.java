/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: LogUtils.java,v 1.1 2004/11/22 18:27:58 kate Exp $
*/

package com.sleepycat.je.log;

import java.nio.ByteBuffer;
import java.sql.Timestamp;

import com.sleepycat.je.utilint.DbLsn;

/**
 * This class holds convenience methods for marshalling internal
 * JE data to and from the log.
 */
public class LogUtils {
    /* storage sizes for int, long in log */
    public static final int INT_BYTES = 4;
    public static final int LONG_BYTES = 8;
    public static final int UNSIGNED_INT_BYTES = 4;

    private static final boolean DEBUG = false;

    /**
     * Marshall a long into the next 4 bytes in this buffer. Necessary
     * when the long is used to hold an unsigned int.
     */
    public static void putUnsignedInt(ByteBuffer buf, long value ) {
        buf.put((byte) (value >>> 0));
        buf.put((byte) (value >>> 8));
        buf.put((byte) (value >>> 16));
        buf.put((byte) (value >>> 24));
    }

    /**
     * Write a long as an unsigned int.
     */
    public static void writeUnsignedInt(ByteBuffer logBuf,
                                        long value) {
        byte b = (byte) (value >>> 0);
        logBuf.put(b);
        b = (byte) (value >>> 8);
        logBuf.put(b);
        b = (byte) (value >>> 16);
        logBuf.put(b);
        b = (byte) (value >>> 24);
        logBuf.put(b);
    }

    /**
     * Unmarshall the next four bytes which hold an unsigned int into a long.
     */
    public static long getUnsignedInt(ByteBuffer buf) {
        return (((buf.get() & 0xFFL) << 0) +
                ((buf.get() & 0xFFL) << 8) +
                ((buf.get() & 0xFFL) << 16) +
                ((buf.get() & 0xFFL) << 24));
    }

    /*
     * Marshall objects.
     */

    /**
     * Write an int into the log
     */
    public static void writeInt(ByteBuffer logBuf, int i) {
        byte b = (byte) ((i >> 0) & 0xff);
        logBuf.put(b);
        b = (byte) ((i >> 8) & 0xff);
        logBuf.put(b);
        b = (byte) ((i >> 16) & 0xff);
        logBuf.put(b);
        b = (byte) ((i >> 24) & 0xff);
        logBuf.put(b);
    }

    /**
     * Read a int from the log.
     */
    public static int readInt(ByteBuffer logBuf) {
        return (((logBuf.get() & 0xFF) << 0) +
                ((logBuf.get() & 0xFF) << 8) +
                ((logBuf.get() & 0xFF) << 16) +
                ((logBuf.get() & 0xFF) << 24));
    }

    /**
     * @return log storage size for a byteArray
     */
    public static int getIntLogSize() {
        return INT_BYTES;
    }

    /**
     * Write a long into the log.
     */
    public static void writeLong(ByteBuffer logBuf, long l) {
        byte b =(byte) (l >>> 0);
        logBuf.put(b);
        b =(byte) (l >>> 8);
        logBuf.put(b);
        b =(byte) (l >>> 16);
        logBuf.put(b);
        b =(byte) (l >>> 24);
        logBuf.put(b);
        b =(byte) (l >>> 32);
        logBuf.put(b);
        b =(byte) (l >>> 40);
        logBuf.put(b);
        b =(byte) (l >>> 48);
        logBuf.put(b);
        b =(byte) (l >>> 56);
        logBuf.put(b);
    }

    /**
     * Read a long from the log.
     */
    public static long readLong(ByteBuffer logBuf) {
        return (((logBuf.get() & 0xFFL) << 0) +
                ((logBuf.get() & 0xFFL) << 8) +
                ((logBuf.get() & 0xFFL) << 16) +
                ((logBuf.get() & 0xFFL) << 24) +
                ((logBuf.get() & 0xFFL) << 32) +
                ((logBuf.get() & 0xFFL) << 40) +
                ((logBuf.get() & 0xFFL) << 48) +
                ((logBuf.get() & 0xFFL) << 56));
    }

    /**
     * @return log storage size for a byteArray
     */
    public static int getLongLogSize() {
        return LONG_BYTES;
    }

    /**
     * Write a byte array into the log. The size is stored first as an 
     * integer
     */
    public static void writeByteArray(ByteBuffer logBuf,
                                      byte[] b) {

        // write the length
        writeInt(logBuf, b.length);

        // Add the data itself
        logBuf.put(b);                     // data
    }

    /**
     * Read a byte array from the log. The size is stored first as an 
     * integer.
     */
    public static byte[] readByteArray(ByteBuffer logBuf) {
        int size = readInt(logBuf);  // how long is it?
        if (DEBUG) {
            System.out.println("pos = " + logBuf.position() +
                               " byteArray is " + size + " on read");
        }
        byte[] b = new byte[size];  
        logBuf.get(b);               // read it out
        return b;                  
    }

    /**
     * @return log storage size for a byteArray
     */
    public static int getByteArrayLogSize(byte[] b) {
        return INT_BYTES + b.length;
    }

    /**
     * Write a string into the log. The size is stored first as an 
     * integer.
     */
    public static void writeString(ByteBuffer logBuf,
                                   String stringVal) {
        writeByteArray(logBuf, stringVal.getBytes());
    }

    /**
     * Read a string from the log. The size is stored first as an 
     * integer.
     */
    public static String readString(ByteBuffer logBuf) {
        return new String(readByteArray(logBuf));
    }

    /**
     * @return log storage size for a string
     */
    public static int getStringLogSize(String s) {
        return INT_BYTES + s.length();
    }

    /**
     * Write a timestamp into the log
     */
    public static void writeTimestamp(ByteBuffer logBuf,
                                      Timestamp time) {
        writeLong(logBuf, time.getTime());
    }

    /**
     * Read a timestamp from the log
     */
    public static Timestamp readTimestamp(ByteBuffer logBuf) {
        long millis = readLong(logBuf);
        return new Timestamp(millis);
    }

    /**
     * @return log storage size for a timestamp
     */
    public static int getTimestampLogSize() {
        return LONG_BYTES;
    }

    /**
     * Write a boolean into the log
     */
    public static void writeBoolean(ByteBuffer logBuf,
                                    boolean bool) {
        byte val = bool ? (byte) 1 : (byte) 0;
        logBuf.put(val);
    }

    /**
     * Read a boolean from the log
     */
    public static boolean readBoolean(ByteBuffer logBuf) {
        byte val = logBuf.get();
        return (val == (byte) 1) ? true : false;
    }

    /**
     * @return log storage size for a boolean
     */
    public static int getBooleanLogSize() {
        return 1;
    }

    /*
     * Dumping support
     */
    public static boolean dumpBoolean(ByteBuffer itemBuffer, StringBuffer sb,
                                      String tag) {
        sb.append("<");
        sb.append(tag);
        sb.append(" exists = \"");
        boolean exists = readBoolean(itemBuffer);
        sb.append(exists);
        if (exists) {
            sb.append("\">");
        } else {
            // close off the tag, we're done
            sb.append("\"/>");
        }
        return exists;
    }

    /**
     * Write a lsn into the log. If it's null, write DbLsn.NULL_LSN in its
     * place.
     */
    public static void writePossiblyNullLsn(ByteBuffer itemBuffer,
					    DbLsn targetLsn) {
        if (targetLsn == null) {
            DbLsn.NULL_LSN.writeToLog(itemBuffer);
        } else {
            targetLsn.writeToLog(itemBuffer);
        }
    }

    /**
     * Read a lsn from the log. If it's DbLsn.NULL_LSN, return null.
     */
    public static DbLsn readPossiblyNullLsn(ByteBuffer itemBuffer) {
    	
        DbLsn readLsn = new DbLsn(0,0);
        readLsn.readFromLog(itemBuffer);
        if (readLsn.compareTo(DbLsn.NULL_LSN) == 0) {
            return null;
        } else {
            return readLsn;
        }
    }

}
