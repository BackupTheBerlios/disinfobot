/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: ForeignKeyNullifier.java,v 1.1 2004/11/22 18:27:59 kate Exp $
*/

package com.sleepycat.je;

/**
 * Javadoc for this public method is generated via
 * the doc templates in the doc_src directory.
 */
public interface ForeignKeyNullifier {

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public boolean nullifyForeignKey(SecondaryDatabase secondary,
                                     DatabaseEntry data)
	throws DatabaseException;
}
