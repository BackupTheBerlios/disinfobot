/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: BINReference.java,v 1.1 2004/11/22 18:27:57 kate Exp $
*/

package com.sleepycat.je.tree;

import java.util.HashSet;
import java.util.Set;

import com.sleepycat.je.dbi.DatabaseId;

/**
 * A class that embodies a reference to a BIN that does not rely on a
 * Java reference to the actual BIN.
 */
public class BINReference {
    protected Key idKey;
    private long nodeId;
    private DatabaseId databaseId;
    private Set deletedKeys;

    BINReference(long nodeId, DatabaseId databaseId, Key idKey) {
	this.nodeId = nodeId;
	this.databaseId = databaseId;
	this.idKey = idKey;
    }

    public long getNodeId() {
	return nodeId;
    }

    public DatabaseId getDatabaseId() {
	return databaseId;
    }

    public Key getKey() {
	return idKey;
    }

    public Key getData() {
	return null;
    }

    public void addDeletedKey(Key key) {

        if (deletedKeys == null) {
            deletedKeys = new HashSet();
        }
        deletedKeys.add(key);
    }

    public void addDeletedKeys(BINReference other) {

        if (deletedKeys == null) {
            deletedKeys = new HashSet();
        }
        if (other.deletedKeys != null) {
            deletedKeys.addAll(other.deletedKeys);
        }
    }

    public void removeDeletedKey(Key key) {

        if (deletedKeys != null) {
            deletedKeys.remove(key);
        }
    }

    public boolean hasDeletedKey(Key key) {

        return (deletedKeys != null) && deletedKeys.contains(key);
    }

    /**
     * Compare two BINReferences.
     */
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (!(obj instanceof BINReference)) {
            return false;
        }

	return ((BINReference) obj).nodeId == nodeId;
    }

    public int hashCode() {
	return (int) nodeId;
    }

    public String toString() {
        return "idKey=" + idKey.getNoFormatString() +
            " nodeId = " + nodeId +
            " db=" + databaseId;
    }
}

