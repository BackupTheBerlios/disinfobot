/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2000-2004
 *      Sleepycat Software.  All rights reserved.
 *
 * $Id: KeyRangeException.java,v 1.1 2004/11/22 18:27:58 kate Exp $
 */

package com.sleepycat.collections;

/**
 * An exception thrown when a key is out of range.
 *
 * @author Mark Hayes
 */
class KeyRangeException extends IllegalArgumentException {

    /**
     * Creates a key range exception.
     */
    public KeyRangeException(String msg) {

        super(msg);
    }
}
