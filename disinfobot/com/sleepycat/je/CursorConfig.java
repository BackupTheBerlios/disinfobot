/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: CursorConfig.java,v 1.1 2004/11/22 18:27:58 kate Exp $
*/

package com.sleepycat.je;

/**
 * Javadoc for this public class is generated
 * via the doc templates in the doc_src directory.
 */
public class CursorConfig implements Cloneable {

    /**
     * Javadoc for this public instance is generated via
     * the doc templates in the doc_src directory.
     */
    public final static CursorConfig DIRTY_READ = new CursorConfig();
    
    static {
        DIRTY_READ.setDirtyRead(true);
    }

    /*
     * For internal use, to allow null as a valid value for
     * the config parameter.
     */
    static CursorConfig DEFAULT = new CursorConfig();

    private boolean dirtyRead = false;

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public CursorConfig() {
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
     * Used by Cursor to create a copy of the application
     * supplied configuration. Done this way to provide non-public cloning.
     */
    CursorConfig cloneConfig() {
        try {
            return (CursorConfig) super.clone();
        } catch (CloneNotSupportedException willNeverOccur) {
            return null;
        }
    }
}
