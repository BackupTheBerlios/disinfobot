/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: FileSelector.java,v 1.1 2004/11/22 18:27:58 kate Exp $
*/

package com.sleepycat.je.cleaner;

import java.util.Set;

import com.sleepycat.je.DatabaseException;

/**
 * Selects files for cleaning.
 */
interface FileSelector {

    /**
     * Returns the file selected for cleaning, or null if none should be
     * cleaned.
     */
    FileRetryInfo getFileToClean(Set excludeFiles, boolean aggressive)
        throws DatabaseException;
}
