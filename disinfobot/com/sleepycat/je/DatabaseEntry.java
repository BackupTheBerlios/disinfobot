/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: DatabaseEntry.java,v 1.1 2004/11/22 18:27:59 kate Exp $
*/

package com.sleepycat.je;

import com.sleepycat.je.tree.TreeUtils;

/**
 * Javadoc for this public class is generated
 * via the doc templates in the doc_src directory.
 */
public class DatabaseEntry {

    /* Currently, JE stores all data records as byte array */
    private byte[] data;
    private int dlen = 0;
    private int doff = 0;
    private int offset = 0;
    private int size = 0;
    private boolean partial = false; 

    public String toString() {
	StringBuffer sb = new StringBuffer("<DatabaseEntry");
	sb.append(" dlen=").append(dlen);
	sb.append(" doff=").append(doff);
	sb.append(" doff=").append(doff);
	sb.append(" offset=").append(offset);
	sb.append(" size=").append(size);
	sb.append(">");
	return sb.toString();
    }

    /*
     * Constructors
     */

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public DatabaseEntry() {
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public DatabaseEntry(byte[] data) {
        this.data = data;
        if (data != null) {
            this.size = data.length;
        }
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public DatabaseEntry(byte[] data, int offset, int size) {
        this.data = data;
        this.offset = offset;
        this.size = size;
    }

    /*
     * Accessors
     */

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public byte[] getData() {
        return data;
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public void setData(byte[] data) {
	this.data = data;
        offset = 0;
	size = (data == null) ? 0 : data.length;
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public void setData(byte[] data, int offset, int size) {
	this.data = data;
        this.offset = offset;
        this.size = size;
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public void setPartial(int doff, int dlen, boolean partial) {
        setPartialOffset(doff);
        setPartialLength(dlen);
        setPartial(partial);
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public int getPartialLength() {
        return dlen;
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public void setPartialLength(int dlen) {
        this.dlen = dlen;
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public int getPartialOffset() {
        return doff;
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public void setPartialOffset(int doff) {
        this.doff = doff;
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public boolean getPartial() {
        return partial;
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public void setPartial(boolean partial) {
        this.partial = partial;
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public int getOffset() {
        return offset;
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public void setOffset(int offset) {
        this.offset = offset;
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public int getSize() {
        return size;
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public void setSize(int size) {
        this.size = size;
    }
    
    /**
     * Dumps the data as a byte array, for tracing purposes
     */
    String dumpData() {
        return TreeUtils.dumpByteArray(data);
    }

    /**
     * Compares the data of two entries.
     */
    boolean dataEquals(DatabaseEntry o) {

        if (this.data == null && (o == null || o.data == null)) {
            return true;
        }
        if (this.data == null || (o == null || o.data == null)) {
            return false;
        }
        if (this.size != o.size) {
            return false;
        }
        for (int i = 0; i < this.size; i += 1) {
            if (this.data[this.offset + i] != o.data[o.offset + i]) {
                return false;
            }
        }
        return true;
    }
}
