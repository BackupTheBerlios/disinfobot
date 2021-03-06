/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: LockMode.java,v 1.1 2004/11/22 18:27:59 kate Exp $
*/

package com.sleepycat.je;

/**
 * Javadoc for this public class is generated
 * via the doc templates in the doc_src directory.
 */
public class LockMode {
    private String lockModeName;

    private LockMode(String lockModeName) {
	this.lockModeName = lockModeName;
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public static final LockMode DEFAULT = new LockMode("DEFAULT");

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public static final LockMode DIRTY_READ = new LockMode("DIRTY_READ");

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public static final LockMode RMW = new LockMode("RMW");

    public String toString() {
	return "LockMode." + lockModeName;
    }
}
