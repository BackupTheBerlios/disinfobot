/*
 * See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: SearchResult.java,v 1.1 2004/11/22 18:27:57 kate Exp $:
 */
package com.sleepycat.je.tree;

/**
 * Contains the result of a tree search
 */
public class SearchResult {
    public boolean exactParentFound;
    public boolean keepSearching;
    public IN parent;
    public int index;
	
    public SearchResult() {
        exactParentFound = false;
        keepSearching = true;
        parent = null;
        index = -1;
    }

    public String toString() {
        return
            "exactParentFound="+ exactParentFound +
            " keepSearching=" + keepSearching +
            " parent=" + ((parent == null)? "null":
                          Long.toString(parent.getNodeId())) +
            " index=" + index;
    }
}
