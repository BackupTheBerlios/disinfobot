/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2000-2004
 *      Sleepycat Software.  All rights reserved.
 *
 * $Id: CharacterBinding.java,v 1.1 2004/11/22 18:27:58 kate Exp $
 */

package com.sleepycat.bind.tuple;

import com.sleepycat.je.DatabaseEntry;

/**
 * A concrete <code>TupleBinding</code> for a <code>Character</code> primitive
 * wrapper or a <code>char</code> primitive.
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
public class CharacterBinding extends TupleBinding {

    private static final int CHAR_SIZE = 2;

    // javadoc is inherited
    public Object entryToObject(TupleInput input) {

        return new Character(input.readChar());
    }

    // javadoc is inherited
    public void objectToEntry(Object object, TupleOutput output) {

        /* Do nothing.  Not called by objectToEntry(Object,DatabaseEntry). */
    }

    // javadoc is inherited
    public void objectToEntry(Object object, DatabaseEntry entry) {

        charToEntry(((Character) object).charValue(), entry);
    }

    /**
     * Converts an entry buffer into a simple <code>char</code> value.
     *
     * @param entry is the source entry buffer.
     *
     * @return the resulting value.
     */
    public static char entryToChar(DatabaseEntry entry) {

        return entryToInput(entry).readChar();
    }

    /**
     * Converts a simple <code>char</code> value into an entry buffer.
     *
     * @param val is the source value.
     *
     * @param entry is the destination entry buffer.
     */
    public static void charToEntry(char val, DatabaseEntry entry) {

        outputToEntry(newOutput(new byte[CHAR_SIZE]).writeChar(val), entry);
    }
}
