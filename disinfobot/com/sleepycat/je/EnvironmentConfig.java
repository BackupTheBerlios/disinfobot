/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: EnvironmentConfig.java,v 1.1 2004/11/22 18:27:58 kate Exp $
*/

package com.sleepycat.je;

import java.util.Properties;

import com.sleepycat.je.config.ConfigParam;
import com.sleepycat.je.config.EnvironmentParams;

/**
 * Javadoc for this public class is generated
 * via the doc templates in the doc_src directory.
 */
public class EnvironmentConfig extends EnvironmentMutableConfig {
    /*
     * For internal use, to allow null as a valid value for
     * the config parameter.
     */
    static EnvironmentConfig DEFAULT = new EnvironmentConfig();

    /**
     * For unit testing, to prevent writing utilization data during checkpoint.
     */
    private boolean checkpointUP = true;

    private boolean allowCreate = false;

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public EnvironmentConfig() {
        super();
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public EnvironmentConfig(Properties properties) 
        throws IllegalArgumentException {

        super(properties);
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
    public void setCacheSize(long totalBytes) 
        throws IllegalArgumentException {

        setVal(EnvironmentParams.MAX_MEMORY, Long.toString(totalBytes));
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public long getCacheSize() {

        /* 
         * CacheSize is filled in from the EnvironmentImpl by way of
         * copyHandleProps.
         */
        return cacheSize;
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public void setCachePercent(int percent) 
        throws IllegalArgumentException {

        setVal(EnvironmentParams.MAX_MEMORY_PERCENT,
               Integer.toString(percent));
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public int getCachePercent() {

        String val = getVal(EnvironmentParams.MAX_MEMORY_PERCENT);
        try {
            return Integer.parseInt(val);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException
		("Cache percent is not a valid integer: " + e.getMessage());
        }
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public void setLockTimeout(long timeout) 
        throws IllegalArgumentException {

        setVal(EnvironmentParams.LOCK_TIMEOUT, Long.toString(timeout));
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public long getLockTimeout() {

        String val = getVal(EnvironmentParams.LOCK_TIMEOUT);
        long timeout = 0;
        try {
            timeout = Long.parseLong(val);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException
		("Bad value for timeout:" + e.getMessage());
        }
        return timeout;
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public void setReadOnly(boolean readOnly) {

        setVal(EnvironmentParams.ENV_RDONLY, Boolean.toString(readOnly));
    } 

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public boolean getReadOnly() {

        String val = getVal(EnvironmentParams.ENV_RDONLY);
        return (Boolean.valueOf(val)).booleanValue();
    } 

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public void setTransactional(boolean transactional) {

        setVal(EnvironmentParams.ENV_INIT_TXN,
	       Boolean.toString(transactional));
    } 

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public boolean getTransactional() {

        String val = getVal(EnvironmentParams.ENV_INIT_TXN);
        return (Boolean.valueOf(val)).booleanValue();
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public void setTxnTimeout(long timeout) 
        throws IllegalArgumentException {

        setVal(EnvironmentParams.TXN_TIMEOUT, Long.toString(timeout));
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public long getTxnTimeout() {

        String val = getVal(EnvironmentParams.TXN_TIMEOUT);
        long timeout = 0;
        try {
            timeout = Long.parseLong(val);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException
		("Bad value for timeout:" + e.getMessage());
        }
        return timeout;
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public void setConfigParam(String paramName,
			       String value) 
        throws IllegalArgumentException {

        /* Override this method to allow setting immutable properties. */
        
        /* Is this a valid property name? */
        ConfigParam param =
            (ConfigParam) EnvironmentParams.SUPPORTED_PARAMS.get(paramName);
        if (param == null) {
            throw new IllegalArgumentException
		(paramName +
		 " is not a valid BDBJE environment configuration");
        }

        setVal(param, value);
    }

    /**
     * For unit testing, to prevent writing utilization data during checkpoint.
     */
    void setCheckpointUP(boolean checkpointUP) {
        this.checkpointUP = checkpointUP;
    }

    /**
     * For unit testing, to prevent writing utilization data during checkpoint.
     */
    boolean getCheckpointUP() {
        return checkpointUP;
    }

    /**
     * Used by Environment to create a copy of the application
     * supplied configuration.
     */
    EnvironmentConfig cloneConfig() {
        try {
            return (EnvironmentConfig) clone();
        } catch (CloneNotSupportedException willNeverOccur) {
            return null;
        }
    }

    public String toString() {
        return ("allowCreate=" + allowCreate + "\n" + super.toString());
    }
}
