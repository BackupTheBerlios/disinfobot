/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2000-2004
 *      Sleepycat Software.  All rights reserved.
 *
 * $Id: StoredMapEntry.java,v 1.1 2004/11/22 18:27:58 kate Exp $
 */

package com.sleepycat.collections;

/**
 * @author Mark Hayes
 */
final class StoredMapEntry extends MapEntryParameter {

    private StoredIterator iter;
    private StoredCollection coll;

    StoredMapEntry(Object key, Object value, StoredCollection coll,
                   StoredIterator iter) {

        super(key, value);
        // Assert: coll, coll.keyBinding/valueBinding
        this.coll = coll;
        this.iter = iter;
    }

    public Object setValue(Object newValue) {

        Object oldValue;
        if (iter != null && iter.isCurrentData(this)) {
            oldValue = getValue();
            iter.set(newValue);
        } else {
            oldValue = coll.put(getKey(), newValue);
        }
        setValueInternal(newValue);
        return oldValue;
    }
}
