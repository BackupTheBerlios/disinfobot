/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: DatabaseUtil.java,v 1.1 2004/11/22 18:27:59 kate Exp $
*/

package com.sleepycat.je;

import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.txn.AutoTxn;
import com.sleepycat.je.txn.BasicLocker;
import com.sleepycat.je.txn.Locker;
import com.sleepycat.je.txn.ThreadLocker;

/**
 * Utils for use in the db package.
 */
class DatabaseUtil {

    /**
     * Get a locker for a writable operation, checking 
     * whether the db and environment is transactional
     * or not. Must return a non null locker.
     */
    static Locker getWritableLocker(Environment env,
                                    Transaction userTxn,
                                    boolean dbIsTransactional)
        throws DatabaseException {

        return getWritableLocker(env, userTxn, dbIsTransactional, false);
    }

    /**
     * Get a locker for a writable operation, also specifying whether to retain
     * non-transactional locks when a new locker must be created.
     * retainNonTxnLocks=true is used for DbTree operations, so that the handle
     * lock may be transferred out of the locker when the operation is
     * complete.
     */
    static Locker getWritableLocker(Environment env,
                                    Transaction userTxn,
                                    boolean dbIsTransactional,
                                    boolean retainNonTxnLocks)
        throws DatabaseException {

        EnvironmentImpl envImpl = env.getEnvironmentImpl();
        boolean envIsTransactional = envImpl.isTransactional();
        if (dbIsTransactional && userTxn == null) {
            return new AutoTxn(envImpl, env.getDefaultTxnConfig());
        } else if (userTxn == null) {
            if (retainNonTxnLocks) {
                return new BasicLocker(envImpl);
            } else {
                return new ThreadLocker(envImpl);
            }
        } else {

            /* 
             * The user provided a transaction, the environment and the
             * database had better be opened transactionally.
             */
            if (!envIsTransactional) {
                throw new DatabaseException
		    ("A Transaction cannot be used because the"+
		     " environment was opened" +
		     " non-transactionally");
            }
            if (!dbIsTransactional) {
                throw new DatabaseException
		    ("A Transaction cannot be used because the" +
		     " database was opened" +
		     " non-transactionally");
            }
            return userTxn.getLocker();
        }
    }
   
    /**
     * Get a locker for a read or cursor operation.
     * See getWritableLocker for an explanation of retainNonTxnLocks.
     */
    static Locker getReadableLocker(Environment env,
                                    Transaction userTxn,
                                    boolean dbIsTransactional,
                                    boolean retainNonTxnLocks) 
        throws DatabaseException {

        if (userTxn != null && !dbIsTransactional) {
            throw new DatabaseException
                ("A Transaction cannot be used because the" +
                 " database was opened" +
                 " non-transactionally");
        }
        Locker locker = (userTxn != null) ? userTxn.getLocker() : null;
        return getReadableLocker(env, locker, retainNonTxnLocks);
    }

    /**
     * Get a locker for this database handle for a read or cursor operation.
     * See getWritableLocker for an explanation of retainNonTxnLocks.
     */
    static Locker getReadableLocker(Environment env,
                                    Database dbHandle,
                                    Locker locker,
                                    boolean retainNonTxnLocks) 
        throws DatabaseException {
        
        if (!dbHandle.isTransactional() && 
            locker != null &&
            locker.isTransactional()) {
            throw new DatabaseException
                ("A Transaction cannot be used because the" +
                 " database was opened" +
                 " non-transactionally");
        }

        return getReadableLocker(env, locker, retainNonTxnLocks);
    }

    /**
     * Get a locker for a read or cursor operation.
     * See getWritableLocker for an explanation of retainNonTxnLocks.
     */
    private static Locker getReadableLocker(Environment env,
                                            Locker locker,
                                            boolean retainNonTxnLocks) 
        throws DatabaseException {

        EnvironmentImpl envImpl = env.getEnvironmentImpl();
        if (locker == null) {
            if (retainNonTxnLocks) {
                return new BasicLocker(envImpl);
            } else {
                return new ThreadLocker(envImpl);
            }
        } else {
            return locker;
        }
    }

    /**
     * Throw an exception if the parameter is null.
     */
    static void checkForNullParam(Object param, String name) {
        if (param == null) {
            throw new NullPointerException(name + " cannot be null");
        }
    }

    /**
     * Throw an exception if the dbt is null or the data field is not set.
     */
    static void checkForNullDbt(DatabaseEntry dbt,
				String name,
				boolean checkData) {
        if (dbt == null) {
            throw new NullPointerException
		("DatabaseEntry " + name + " cannot be null");
        }

        if (checkData) {
            if (dbt.getData() == null) {
                throw new NullPointerException
		    ("Data field for DatabaseEntry " +
		     name + " cannot be null");
            }
        }
    }

    /**
     * Throw an exception if the key dbt has the partial flag set.  This method
     * should be called for all put() operations.
     */
    static void checkForPartialKey(DatabaseEntry dbt) {
        if (dbt.getPartial()) {
            throw new IllegalArgumentException
		("A partial key DatabaseEntry is not allowed");
        }
    }
}
