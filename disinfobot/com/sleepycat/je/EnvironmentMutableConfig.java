/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: EnvironmentMutableConfig.java,v 1.1 2004/11/22 18:27:59 kate Exp $
*/

package com.sleepycat.je;

import java.util.Enumeration;
import java.util.Iterator;
import java.util.Properties;

import com.sleepycat.je.config.ConfigParam;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.EnvironmentImpl;

/**
 * Javadoc for this public class is generated
 * via the doc templates in the doc_src directory.
 */
public class EnvironmentMutableConfig implements Cloneable {

    /* Change copyHandlePropsTo when adding fields here. */
    private boolean txnNoSync = false;

    /* 
     * Cache size is a category of property that is calculated
     * within the environment. It will be mutable in the future.
     * It is only supplied when returning the cache size to the application
     * and never used internally; internal code directly checks with
     * the MemoryBudget class;
     */
    protected long cacheSize;

    /**
     * Note that in the implementation we choose not to extend Properties 
     * in order to keep the configuration type safe.
     */
    private Properties props;

    /**
     * For unit testing, to prevent loading of je.properties.
     */
    private boolean loadPropertyFile = true;

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public EnvironmentMutableConfig() {
        props = new Properties();
    }

    /**
     * Used by EnvironmentConfig to construct from properties.
     */
    EnvironmentMutableConfig(Properties properties)
        throws IllegalArgumentException {

        validateProperties(properties);
        /* For safety, copy the passed in properties. */
        props = new Properties();
        props.putAll(properties);
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public void setTxnNoSync(boolean noSync) {
        txnNoSync = noSync;
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public boolean getTxnNoSync() {
        return txnNoSync;
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public void setConfigParam(String paramName,
			       String value) 
        throws IllegalArgumentException {
        
        /* Is this a valid property name? */
        ConfigParam param =
            (ConfigParam) EnvironmentParams.SUPPORTED_PARAMS.get(paramName);
        if (param == null) {
            throw new IllegalArgumentException
		(paramName +
		 " is not a valid BDBJE environment configuration");
        }
        /* Is this a mutable property? */
        if (!param.isMutable()) {
            throw new IllegalArgumentException
		(paramName +
		 " is not a mutable BDBJE environment configuration");
        }

        setVal(param, value);
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public String getConfigParam(String paramName)
        throws IllegalArgumentException {
        
        /* Is this a valid property name? */
        ConfigParam param =
            (ConfigParam) EnvironmentParams.SUPPORTED_PARAMS.get(paramName);
        if (param == null) {
            throw new IllegalArgumentException
		(paramName +
		 " is not a valid BDBJE environment configuration");
        }

        return getVal(param);
    }

    /*
     * Helpers
     */

    /**
     * Gets either the value stored in this configuration or the
     * default value for this param.
     */   
    String getVal(ConfigParam param) {
        String val = props.getProperty(param.getName());
        if (val == null) {
            val = param.getDefault();
        }
        return val;
    }

    /**
     * Sets and validates the specified parameter.
     */
    void setVal(ConfigParam param, String val)
        throws IllegalArgumentException {

        param.validateValue(val);
        props.setProperty(param.getName(), val);
    }
    
    /**
     * Validate a property bag passed in a construction time.
     */
    void validateProperties(Properties props)
        throws IllegalArgumentException {

        /* Check that the properties have valid names and values */
        Enumeration propNames = props.propertyNames();
        while (propNames.hasMoreElements()) {
            String name = (String) propNames.nextElement();
            /* Is this a valid property name? */
            ConfigParam param =
                (ConfigParam) EnvironmentParams.SUPPORTED_PARAMS.get(name);
            if (param == null) {
                throw new IllegalArgumentException
		    (name + " is not a valid BDBJE environment configuration");
            }
            /* Is this a valid property value? */
            param.validateValue(props.getProperty(name));
        }
    }

    /**
     * Check that the immutable values in the environment config used to open
     * an environment match those in the config object saved by the underlying
     * shared EnvironmentImpl.
     */
    void checkImmutablePropsForEquality(EnvironmentMutableConfig passedConfig)
        throws IllegalArgumentException {

        Properties passedProps = passedConfig.props;
        Iterator iter = EnvironmentParams.SUPPORTED_PARAMS.keySet().iterator();
        while (iter.hasNext()) {
            String paramName = (String) iter.next();
            ConfigParam param = (ConfigParam)
                EnvironmentParams.SUPPORTED_PARAMS.get(paramName);
            assert param != null;
            if (!param.isMutable()) {
                String paramVal = props.getProperty(paramName);
                String useParamVal = passedProps.getProperty(paramName);
                if ((paramVal != null) ? (!paramVal.equals(useParamVal))
                                       : (useParamVal != null)) {
                    throw new IllegalArgumentException
                        (paramName + " is set to " +
                         useParamVal +
                         " in the config parameter" +
                         " which is incompatible " +
                         " with the value of " +
                         paramVal + " in the " +
                         " underlying environment");
                }
            }
        }
    }

    /**
     * Overrides Object.clone() to clone all properties, used by this class and
     * EnvironmentConfig.
     */
    protected Object clone()
        throws CloneNotSupportedException {

        EnvironmentMutableConfig copy =
            (EnvironmentMutableConfig) super.clone();
        copy.props = (Properties) props.clone();
        return copy;
    }

    /**
     * Used by Environment to create a copy of the application
     * supplied configuration. Done this way to provide non-public cloning.
     */
    EnvironmentMutableConfig cloneMutableConfig() {
        try {
            EnvironmentMutableConfig copy = (EnvironmentMutableConfig) clone();
            /* Remove all immutable properties. */
            copy.clearImmutableProps();
            return copy;
        } catch (CloneNotSupportedException willNeverOccur) {
            return null;
        }
    }

    /**
     * Copies the per-handle properties of this object to the given config
     * object.
     */
    void copyHandlePropsTo(EnvironmentMutableConfig other) {
        other.txnNoSync = txnNoSync;
    }

    /**
     * Copies all mutable props to the given config object.
     */
    void copyMutablePropsTo(EnvironmentMutableConfig toConfig) {

        Properties toProps = toConfig.props;
        Enumeration propNames = props.propertyNames();
        while (propNames.hasMoreElements()) {
            String paramName = (String) propNames.nextElement();
            ConfigParam param = (ConfigParam)
                EnvironmentParams.SUPPORTED_PARAMS.get(paramName);
            assert param != null;
            if (param.isMutable()) {
                String newVal = props.getProperty(paramName);
                toProps.setProperty(paramName, newVal);
            }
        }
    }

    /**
     * Fill in the properties calculated by the environment to the given
     * config object.
     */
    void fillInEnvironmentGeneratedProps(EnvironmentImpl envImpl) {
        cacheSize = envImpl.getMemoryBudget().getMaxMemory();
    }

    /**
     * Removes all immutable props.
     */
    private void clearImmutableProps() {
        Enumeration propNames = props.propertyNames();
        while (propNames.hasMoreElements()) {
            String paramName = (String) propNames.nextElement();
            ConfigParam param = (ConfigParam)
                EnvironmentParams.SUPPORTED_PARAMS.get(paramName);
            assert param != null;
            if (!param.isMutable()) {
                props.remove(paramName);
            }
        }
    }

    /**
     * For unit testing, to prevent loading of je.properties.
     */
    void setLoadPropertyFile(boolean loadPropertyFile) {
        this.loadPropertyFile = loadPropertyFile;
    }

    /**
     * For unit testing, to prevent loading of je.properties.
     */
    boolean getLoadPropertyFile() {
        return loadPropertyFile;
    }

    /**
     * Testing support
     */
    int getNumExplicitlySetParams() {
        return props.size();
    }

    public String toString() {
        return props.toString();
    }
}
