/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: OperationStatus.java,v 1.1 2004/11/22 18:27:59 kate Exp $
*/

package com.sleepycat.je;

/**
 * Javadoc for this public class is generated
 * via the doc templates in the doc_src directory.
 */
public class OperationStatus {
    /**
     * Javadoc for this public instance is generated via
     * the doc templates in the doc_src directory.
     */
    public static final OperationStatus SUCCESS =
	new OperationStatus("SUCCESS");

    /**
     * Javadoc for this public instance is generated via
     * the doc templates in the doc_src directory.
     */
    public static final OperationStatus KEYEXIST =
	new OperationStatus("KEYEXIST");

    /**
     * Javadoc for this public instance is generated via
     * the doc templates in the doc_src directory.
     */
    public static final OperationStatus KEYEMPTY =
	new OperationStatus("KEYEMPTY");

    /**
     * Javadoc for this public instance is generated via
     * the doc templates in the doc_src directory.
     */
    public static final OperationStatus NOTFOUND =
	new OperationStatus("NOTFOUND");

    /* For toString */
    private String statusName;

    private OperationStatus(String statusName) {
	this.statusName = statusName;
    }

    public String toString() {
	return "OperationStatus." + statusName;
    }
}
