/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: DatabaseConfig.java,v 1.1 2004/11/22 18:27:59 kate Exp $
*/

package com.sleepycat.je;

import java.util.Comparator;

import com.sleepycat.je.dbi.DatabaseImpl;

/**
 * Javadoc for this public class is generated
 * via the doc templates in the doc_src directory.
 */
public class DatabaseConfig implements Cloneable {
    /*
     * For internal use, to allow null as a valid value for
     * the config parameter.
     */
    static DatabaseConfig DEFAULT = new DatabaseConfig();

    private boolean allowCreate = false;
    private boolean exclusiveCreate = false;
    private boolean transactional = false;
    private boolean readOnly = false;
    private boolean duplicatesAllowed = false;

    /* User defined Btree and duplicate comparison functions, if specified.*/
    private Comparator btreeComparator = null;
    private Comparator duplicateComparator = null;
    private boolean overrideBtreeComparator = false;
    private boolean overrideDupComparator = false;
    private boolean useExistingConfig = false;

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public DatabaseConfig() {
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public void setAllowCreate(boolean allowCreate) {
        this.allowCreate = allowCreate;
    } 

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public boolean getAllowCreate() {
        return allowCreate;
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public void setExclusiveCreate(boolean exclusiveCreate) {
        this.exclusiveCreate = exclusiveCreate;
    } 

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public boolean getExclusiveCreate() {
        return exclusiveCreate;
    } 

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public void setSortedDuplicates(boolean duplicatesAllowed) {
        this.duplicatesAllowed = duplicatesAllowed;
    } 

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public boolean getSortedDuplicates() {
        return duplicatesAllowed;
    } 

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public void setTransactional(boolean transactional) {

        this.transactional = transactional;
    } 
    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public boolean getTransactional() {

        return transactional;
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    } 

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public boolean getReadOnly() {
        return readOnly;
    } 

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public void setBtreeComparator(Class btreeComparator) {
        /* Note: comparator may be null */
        this.btreeComparator = validateComparator(btreeComparator, "Btree");
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public Comparator getBtreeComparator() {
        return btreeComparator;
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public void setOverrideBtreeComparator(boolean override) {
        overrideBtreeComparator = override;
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public boolean getOverrideBtreeComparator() {
        return overrideBtreeComparator;
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public void setDuplicateComparator(Class duplicateComparator) {
        /* Note: comparator may be null */
        this.duplicateComparator =
	    validateComparator(duplicateComparator, "Duplicate");
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public Comparator getDuplicateComparator() {
        return duplicateComparator;
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public void setOverrideDuplicateComparator(boolean override) {
        overrideDupComparator = override;
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public boolean getOverrideDuplicateComparator() {
        return overrideDupComparator;
    }

    /**
     * For utilities, to avoid having to know the configuration of a database.
     */
    void setUseExistingConfig(boolean useExistingConfig) {
        this.useExistingConfig = useExistingConfig;
    }

    /**
     * For utilities, to avoid having to know the configuration of a database.
     */
    boolean getUseExistingConfig() {
        return useExistingConfig;
    }

    /**
     * Used by Database to create a copy of the application
     * supplied configuration. Done this way to provide non-public cloning.
     */
    DatabaseConfig cloneConfig() {
        try {
            return (DatabaseConfig) super.clone();
        } catch (CloneNotSupportedException willNeverOccur) {
            return null;
        }
    }

    /**
     * Check that this comparator can be instantiated by JE.
     */
    private Comparator validateComparator(Class comparator, String type) 
        throws IllegalArgumentException {

	if (comparator == null) {
	    return null;
	}

        try {
	    Comparator ret =
		DatabaseImpl.instantiateComparator(comparator, type);
	    if (ret instanceof Comparator) {
		return ret;
	    } else {
		throw new IllegalArgumentException
		    (comparator.getName() +
		     " is is not valid as a " + type +
		     " comparator because it does not " +
		     " implement java.util.Comparator.");
	    }
        } catch (DatabaseException e) {
            throw new IllegalArgumentException
		(type +
		 " comparator is not valid: " +
		 e.getMessage() +
		 "\nPerhaps you have not implemented a zero-parameter " +
		 "constructor for the comparator or the comparator class " +
		 "cannot be found.");
        }
    }
}
