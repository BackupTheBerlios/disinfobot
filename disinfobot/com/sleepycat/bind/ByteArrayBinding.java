/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2000-2004
 *      Sleepycat Software.  All rights reserved.
 *
 * $Id: ByteArrayBinding.java,v 1.1 2004/11/22 18:27:57 kate Exp $
 */

package com.sleepycat.bind;

import com.sleepycat.je.DatabaseEntry;

/**
 * A pass-through <code>EntryBinding</code> that uses the entry's byte array as
 * the key or data object.
 *
 * @author Mark Hayes
 */
public class ByteArrayBinding implements EntryBinding {

    /**
     * Creates a byte array binding.
     */
    public ByteArrayBinding() {
    }

    // javadoc is inherited
    public Object entryToObject(DatabaseEntry entry) {

        byte[] bytes = new byte[entry.getSize()];
        System.arraycopy(entry.getData(), entry.getOffset(),
                         bytes, 0, bytes.length);
        return bytes;
    }

    // javadoc is inherited
    public void objectToEntry(Object object, DatabaseEntry entry) {

        byte[] bytes = (byte[]) object;
        entry.setData(bytes, 0, bytes.length);
    }
}
