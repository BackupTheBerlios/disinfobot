/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: SecondaryConfig.java,v 1.1 2004/11/22 18:27:59 kate Exp $
*/

package com.sleepycat.je;

/**
 * Javadoc for this public class is generated via
 * the doc templates in the doc_src directory.
 */
public class SecondaryConfig extends DatabaseConfig {

    /*
     * For internal use, to allow null as a valid value for
     * the config parameter.
     */
    static SecondaryConfig DEFAULT = new SecondaryConfig();

    private boolean allowPopulate;
    private SecondaryKeyCreator keyCreator;
    private Database foreignKeyDatabase;
    private ForeignKeyDeleteAction foreignKeyDeleteAction =
            ForeignKeyDeleteAction.ABORT;
    private ForeignKeyNullifier foreignKeyNullifier;

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public SecondaryConfig() {
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public void setKeyCreator(SecondaryKeyCreator keyCreator) {
        this.keyCreator = keyCreator;
    } 

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public SecondaryKeyCreator getKeyCreator() {
        return keyCreator;
    } 

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public void setAllowPopulate(boolean allowPopulate) {
        this.allowPopulate = allowPopulate;
    } 

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public boolean getAllowPopulate() {
        return allowPopulate;
    } 

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public void setForeignKeyDatabase(Database foreignKeyDatabase) {
        this.foreignKeyDatabase = foreignKeyDatabase;
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public Database getForeignKeyDatabase() {
        return foreignKeyDatabase;
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public void setForeignKeyDeleteAction(ForeignKeyDeleteAction
                                          foreignKeyDeleteAction) {
        DatabaseUtil.checkForNullParam(foreignKeyDeleteAction,
                                       "foreignKeyDeleteAction");
        this.foreignKeyDeleteAction = foreignKeyDeleteAction;
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public ForeignKeyDeleteAction getForeignKeyDeleteAction() {
        return foreignKeyDeleteAction;
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public void setForeignKeyNullifier(ForeignKeyNullifier
                                       foreignKeyNullifier) {
        this.foreignKeyNullifier = foreignKeyNullifier;
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public ForeignKeyNullifier getForeignKeyNullifier() {
        return foreignKeyNullifier;
    }
}
