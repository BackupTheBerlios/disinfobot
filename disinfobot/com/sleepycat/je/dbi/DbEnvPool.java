/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2000-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: DbEnvPool.java,v 1.1 2004/11/22 18:27:59 kate Exp $
*/

package com.sleepycat.je.dbi;

import java.io.File;
import java.util.Hashtable;
import java.util.Map;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentConfig;

/**
 * Singleton collection of database environments.
 */
public class DbEnvPool {
    // singleton instance.
    private static DbEnvPool pool = new DbEnvPool();

    /* Collection of env handles, mapped by directory name->env object. */
    private Map envs;

    /**
     * Enforce singleton behavior.
     */
    private DbEnvPool() {
        envs = new Hashtable();
    }

    /**
     * Access the singleton instance.
     */
    public static DbEnvPool getInstance() {
        return pool;
    }

    /**
     * Find a single environment, used by Environment handles and by command
     * line utilities.
     */
    public synchronized
        EnvironmentImplInfo getEnvironment(File dbEnvHome,
                                           EnvironmentConfig config )
        throws DatabaseException {

        boolean firstHandle = false;
        EnvironmentImpl environmentImpl = null;
        if (envs.containsKey(dbEnvHome)) {
            /* Environment is resident */
            environmentImpl = (EnvironmentImpl) envs.get(dbEnvHome);
            if (!environmentImpl.isOpen()) {
                environmentImpl.open();
            }
        } else {
            /* 
	     * Environment must be instantiated. If it can be created,
             * the configuration must have allowedCreate set.
             */
            environmentImpl = new EnvironmentImpl(dbEnvHome, config);
            envs.put(dbEnvHome, environmentImpl);
            firstHandle = true;
        }

        return new EnvironmentImplInfo(environmentImpl, firstHandle);
    }


    /**
     * Remove a EnvironmentImpl from the pool because it's been closed.
     */
    void remove(File dbEnvHome) {
        envs.remove(dbEnvHome);
    }

    public void clear() {
        envs.clear();
    }

    /* 
     * Struct for returning two values.
     */
    public static class EnvironmentImplInfo {
        public EnvironmentImpl envImpl;
        public boolean firstHandle = false;

        EnvironmentImplInfo(EnvironmentImpl envImpl, boolean firstHandle) {
            this.envImpl = envImpl;
            this.firstHandle = firstHandle;
        }
    }
}
