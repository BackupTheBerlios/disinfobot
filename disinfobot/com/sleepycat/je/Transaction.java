/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: Transaction.java,v 1.1 2004/11/22 18:27:59 kate Exp $
*/

package com.sleepycat.je;

import com.sleepycat.je.txn.Locker;
import com.sleepycat.je.txn.Txn;
import com.sleepycat.je.utilint.PropUtil;

/**
 * Javadoc for this public class is generated
 * via the doc templates in the doc_src directory.
 */
public class Transaction {

    private Txn txn;
    private Environment env;
    private String name;

    /**
     * Creates a transaction.
     */
    Transaction(Environment env, Txn txn, String name) {
        this.env = env;
        this.txn = txn;
	this.name = name;
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public void abort()
	throws DatabaseException {

        checkEnv();
        env.removeReferringHandle(this);
        txn.abort();
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public long getId() 
        throws DatabaseException {

        return txn.getId();
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public void commit()
	throws DatabaseException {

        checkEnv();
        env.removeReferringHandle(this);
        txn.commit();
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public void commitSync()
	throws DatabaseException {

        checkEnv();
        env.removeReferringHandle(this);
        txn.commit(true);
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public void commitNoSync()
	throws DatabaseException {

        checkEnv();
        env.removeReferringHandle(this);
        txn.commit(false);
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public void setTxnTimeout(long timeOut) 
        throws DatabaseException {

        checkEnv();
        txn.setTxnTimeout(PropUtil.microsToMillis(timeOut));
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public void setLockTimeout(long timeOut) 
        throws DatabaseException {

        checkEnv();
        txn.setLockTimeout(PropUtil.microsToMillis(timeOut));
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public void setName(String name) {
	this.name = name;
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public String getName() {
	return name;
    }

    public String toString() {
	StringBuffer sb = new StringBuffer();
	sb.append("<Transaction id=\"");
	sb.append(txn.getId()).append("\"");
	if (name != null) {
	    sb.append(" name=\"");
	    sb.append(name).append("\"");
	    }
	sb.append(">");
	return sb.toString();
    }

    /**
     * Internal use only.
     */
    Locker getLocker() {
        return txn;
    }

    /*
     * Helpers
     */

    /**
     * @throws RunRecoveryException if the underlying environment is invalid.
     */
    private void checkEnv() 
        throws RunRecoveryException {
        
	env.getEnvironmentImpl().checkIfInvalid();
    }
}
