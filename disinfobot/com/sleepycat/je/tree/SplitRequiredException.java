/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: SplitRequiredException.java,v 1.1 2004/11/22 18:27:57 kate Exp $
*/

package com.sleepycat.je.tree;

/**
 * Indicates that we need to return to the top of the tree in order to
 * do a forced splitting pass.
 */
class SplitRequiredException extends Exception {
    public SplitRequiredException(){
    }
}
