/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: TransactionConfig.java,v 1.1 2004/11/22 18:27:59 kate Exp $
*/

package com.sleepycat.je;

/**
 * Javadoc for this public class is generated
 * via the doc templates in the doc_src directory.
 */
public class TransactionConfig implements Cloneable {
    /*
     * For internal use, to allow null as a valid value for
     * the config parameter.
     */
    static TransactionConfig DEFAULT = new TransactionConfig();

    private boolean sync = false;
    private boolean noSync = false;
    private boolean noWait = false;
    private boolean dirtyRead = false;

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public TransactionConfig() {
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public void setSync(boolean sync) {
        this.sync = sync;
    } 

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public boolean getSync() {
        return sync;
    } 

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public void setNoSync(boolean noSync) {
        this.noSync = noSync;
    } 

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public boolean getNoSync() {
        return noSync;
    } 

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public void setNoWait(boolean noWait) {
        this.noWait = noWait;
    } 

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public boolean getNoWait() {
        return noWait;
    } 

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public void setDirtyRead(boolean dirtyRead) {
        this.dirtyRead = dirtyRead;
    } 

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public boolean getDirtyRead() {
        return dirtyRead;
    } 

    /**
     * Used by Environment to create a copy of the application
     * supplied configuration. Done this way to provide non-public cloning.
     */
    TransactionConfig cloneConfig() {
        try {
            return (TransactionConfig) super.clone();
        } catch (CloneNotSupportedException willNeverOccur) {
            return null;
        }
    }
}
