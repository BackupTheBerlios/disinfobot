/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2000-2004
 *      Sleepycat Software.  All rights reserved.
 *
 * $Id: ShortBinding.java,v 1.1 2004/11/22 18:27:58 kate Exp $
 */

package com.sleepycat.bind.tuple;

import com.sleepycat.je.DatabaseEntry;

/**
 * A concrete <code>TupleBinding</code> for a <code>Short</code> primitive
 * wrapper or a <code>short</code> primitive.
 *
 * <p>There are two ways to use this class:</p>
 * <ol>
 * <li>When using the {@link com.sleepycat.je} package directly, the static
 * methods in this class can be used to convert between primitive values and
 * {@link DatabaseEntry} objects.</li>
 * <li>When using the {@link com.sleepycat.collections} package, an instance of
 * this class can be used with any stored collection.  The easiest way to
 * obtain a binding instance is with the {@link
 * TupleBinding#getPrimitiveBinding} method.</li>
 * </ol>
 */
public class ShortBinding extends TupleBinding {

    private static final int SHORT_SIZE = 2;

    // javadoc is inherited
    public Object entryToObject(TupleInput input) {

        return new Short(input.readShort());
    }

    // javadoc is inherited
    public void objectToEntry(Object object, TupleOutput output) {

        /* Do nothing.  Not called by objectToEntry(Object,DatabaseEntry). */
    }

    // javadoc is inherited
    public void objectToEntry(Object object, DatabaseEntry entry) {

        shortToEntry(((Number) object).shortValue(), entry);
    }

    /**
     * Converts an entry buffer into a simple <code>short</code> value.
     *
     * @param entry is the source entry buffer.
     *
     * @return the resulting value.
     */
    public static short entryToShort(DatabaseEntry entry) {

        return entryToInput(entry).readShort();
    }

    /**
     * Converts a simple <code>short</code> value into an entry buffer.
     *
     * @param val is the source value.
     *
     * @param entry is the destination entry buffer.
     */
    public static void shortToEntry(short val, DatabaseEntry entry) {

        outputToEntry(newOutput(new byte[SHORT_SIZE]).writeShort(val), entry);
    }
}
