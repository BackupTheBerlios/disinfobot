/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: LN.java,v 1.1 2004/11/22 18:27:57 kate Exp $
*/

package com.sleepycat.je.tree;

import java.nio.ByteBuffer;
import java.util.Map;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.INList;
import com.sleepycat.je.dbi.MemoryBudget;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogException;
import com.sleepycat.je.log.LogManager;
import com.sleepycat.je.log.LogReadable;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.log.LoggableObject;
import com.sleepycat.je.log.entry.DeletedDupLNLogEntry;
import com.sleepycat.je.log.entry.LNLogEntry;
import com.sleepycat.je.txn.Locker;
import com.sleepycat.je.txn.Txn;
import com.sleepycat.je.utilint.DbLsn;

/**
 * An LN represents a Leaf Node in the JE tree.
 */
public class LN extends Node implements LoggableObject, LogReadable {
    private static final String BEGIN_TAG = "<ln>";
    private static final String END_TAG = "</ln>";
    
    private byte[] data;

    /**
     * Create an empty LN, to be filled in from the log.
     */
    public LN() {
        super(false);
        this.data = null;
    }
    
    /**
     * Create a new LN from a byte array.
     */
    public LN(byte[] data) {
        super(true);
        if (data == null) {
            this.data = null;
        } else {
            init(data, 0, data.length);
        }
    }
    
    /**
     * Create a new LN from a DatabaseEntry.
     */
    public LN(DatabaseEntry dbt) {
        super(true);
        byte[] data = dbt.getData();
        if (data == null) {
            this.data = null;
        } else if (dbt.getPartial()) {
            init(data,
                 dbt.getOffset(),
                 dbt.getPartialOffset() + dbt.getSize(),
                 dbt.getPartialOffset(),
                 dbt.getSize());
        } else {
            init(data, dbt.getOffset(), dbt.getSize());
        }
    }

    private void init(byte[] data, int off, int len, int doff, int dlen) {
        this.data = new byte[len];
        System.arraycopy(data, off, this.data, doff, dlen);
    }

    private void init(byte[] data, int off, int len) {
        init(data, off, len, 0, len);
    }

    public byte[] getData() {
        return data;
    }
    
    public byte[] copyData() {
        int len = data.length;
        byte[] ret = new byte[len];
        System.arraycopy(data, 0, ret, 0, len);
        return ret;
    }

    public boolean isDeleted() {
        return (data == null);
    }

    void makeDeleted() {
        data = null;
    }

    /* 
     * If you get to an LN, this subtree isn't valid for delete. True,
     * the LN may have been deleted, but you can't be sure without taking
     * a lock, and the validate -subtree-for-delete process assumes that
     * bin compressing has happened and there are no committed, deleted
     * LNS hanging off the BIN.
     */
    boolean isValidForDelete() {
        return false;
    }

    /**
     * A LN can never be a child in the search chain.
     */
    protected boolean isSoughtNode(long nid) {
        return false;
    }

    /**
     * A LN can never be the ancestor of another node.
     */
    protected boolean canBeAncestor(IN target) {
        return false;
    }

    /**
     * Delete this LN's data and log the new version. 
     */
    public DbLsn delete(DatabaseImpl database,
                        Key lnKey,
                        Key dupKey,
                        DbLsn oldLsn,
                        Locker locker)
        throws DatabaseException {

        makeDeleted();
        EnvironmentImpl env = database.getDbEnvironment();

        DbLsn newLsn = null;
        if (dupKey != null) {

            /*
             * Deleted Duplicate LNs are logged with two keys -- the one
             * that identifies the main tree (the dup key) and the
             * one that places them in the duplicate tree (really the data)
             * since we can't recreate the latter  because the data field
             * has been nulled. Note that the dupKey is passed to the
             * log manager FIRST, because the dup key is the one that 
             * navigates us in the main tree. The "key" is the one that
             * navigates us in the duplicate tree. Also, we must check 
             * if this is a transactional entry that must be rolled back 
             * or one done on the behalf of a null txn.
             */
            LogEntryType entryType;
            DbLsn logAbortLsn;
            boolean logAbortKnownDeleted;
            Txn logTxn;
            if (locker instanceof Txn) {
                entryType = LogEntryType.LOG_DEL_DUPLN_TRANSACTIONAL;
                logAbortLsn = locker.getAbortLsn(getNodeId());
                logAbortKnownDeleted =
                    locker.getAbortKnownDeleted(getNodeId());
                logTxn = (Txn) locker;
            } else {
                entryType = LogEntryType.LOG_DEL_DUPLN;
                logAbortLsn = null;
                logAbortKnownDeleted = true;
                logTxn = null;
            }

            /* Don't count oldLsn as obsolete if it's already deleted. */
            if (oldLsn != null &&
                oldLsn.equals(logAbortLsn) &&
                logAbortKnownDeleted) {
                oldLsn = null;
            }

            DeletedDupLNLogEntry logEntry =
                new DeletedDupLNLogEntry(entryType,
                                         this,
                                         database.getId(),
                                         dupKey,
                                         lnKey,
                                         logAbortLsn,
                                         logAbortKnownDeleted,
                                         logTxn);

            LogManager logManager = env.getLogManager();
            newLsn = logManager.log(logEntry, false, oldLsn, true);

        } else {

            /*
             * Non duplicate LN, just log the normal way.
             */
            newLsn = log(env, database.getId(), lnKey, oldLsn, locker);
        }
        return newLsn;
    }

    /**
     * Modify the LN's data and log the new version.
     */
    public DbLsn modify(byte[] newData,
                        DatabaseImpl database,
                        Key lnKey,
                        DbLsn oldLsn,
                        Locker locker)
        throws DatabaseException {

        data = newData;

        /* Log the new ln. */
        EnvironmentImpl env = database.getDbEnvironment();
        DbLsn newLsn = log(env, database.getId(), lnKey, oldLsn, locker);
        return newLsn;
    }

    /**
     * Add yourself to the dirty list if you're dirty. LNs are never dirty.
     */
    void addToDirtyMap(Map dirtyMap) {
    }

    /**
     * Add yourself to the in memory list if you're a type of node that 
     * should belong.
     */
    void rebuildINList(INList inList) {
        // don't add, LNs don't belong on the list.
    }

    /**
     * No need to do anything, stop the search.
     */
    void removeFromINList(INList inList) {
        // don't remove, LNs not on this list.
    }

    /**
     * Compute the approximate size of this node in memory for evictor
     * invocation purposes.
     */
    public long getMemorySizeIncludedByParent() {
        long size = MemoryBudget.LN_OVERHEAD;
        if (data != null) {
            size += data.length + MemoryBudget.BYTE_ARRAY_OVERHEAD;
        } 
        return size;
    }

    /*
     * Dumping
     */

    public String beginTag() {
        return BEGIN_TAG;
    }

    public String endTag() {
        return END_TAG;
    }

    public String dumpString(int nSpaces, boolean dumpTags) {
        StringBuffer self = new StringBuffer();
        if (dumpTags) {
	    self.append(TreeUtils.indent(nSpaces));
            self.append(beginTag());
            self.append('\n');
        }

        self.append(super.dumpString(nSpaces + 2, true));
        self.append('\n');
        if (data != null) {
            self.append(TreeUtils.indent(nSpaces+2));
            self.append("<data>");
            self.append(TreeUtils.dumpByteArray(data));
            self.append("</data>");
            self.append('\n');
        }
        if (dumpTags) {
            self.append(TreeUtils.indent(nSpaces));
            self.append(endTag());
        }
        return self.toString();
    }

    /*
     * Logging Support
     */

    /**
     * Log a provisional, non-txnal version of a ln.
     * @param env the environment.
     * @param dbId database id of this node. (Not stored in LN)
     * @param key key of this node. (Not stored in LN)
     * @param oldLsn is the LSN of the previous version or null.
     */
    public DbLsn logProvisional(EnvironmentImpl env,
                                DatabaseId dbId,
                                Key key, 
                                DbLsn oldLsn)
        throws DatabaseException {
        return log(env, dbId, key, oldLsn, null, true);
    }

    /**
     * Log this LN. Whether its logged as
     * a transactional entry or not depends on the type of locker.
     * @param env the environment.
     * @param dbId database id of this node. (Not stored in LN)
     * @param key key of this node. (Not stored in LN)
     * @param oldLsn is the LSN of the previous version or null.
     * @param locker owning locker.
     */
    public DbLsn log(EnvironmentImpl env,
                     DatabaseId dbId,
                     Key key,
                     DbLsn oldLsn,
                     Locker locker)
        throws DatabaseException {

        return log(env, dbId, key, oldLsn, locker, false);
    }

    /**
     * Log this LN. Whether its logged as
     * a transactional entry or not depends on the type of locker.
     * @param env the environment.
     * @param dbId database id of this node. (Not stored in LN)
     * @param key key of this node. (Not stored in LN)
     * @param oldLsn is the LSN of the previous version or null.
     * @param locker owning locker.
     */
    private DbLsn log(EnvironmentImpl env,
                      DatabaseId dbId,
                      Key key,
                      DbLsn oldLsn,
                      Locker locker,
                      boolean isProvisional)
        throws DatabaseException {

        LogEntryType entryType;
        DbLsn logAbortLsn;
	boolean logAbortKnownDeleted;
        Txn logTxn;
        if (locker instanceof Txn) {
            entryType = getTransactionalLogType();
            logAbortLsn = locker.getAbortLsn(getNodeId());
            logAbortKnownDeleted = locker.getAbortKnownDeleted(getNodeId());
            logTxn = (Txn)locker;
        } else {
            entryType = getLogType();
            logAbortLsn = null;
	    logAbortKnownDeleted = false;
            logTxn = null;
        }

        /* Don't count oldLsn as obsolete if it's already deleted. */
        if (oldLsn != null &&
            oldLsn.equals(logAbortLsn) &&
            logAbortKnownDeleted) {
            oldLsn = null;
        }

        LNLogEntry logEntry = new LNLogEntry(entryType, 
                                             this,
                                             dbId,
                                             key,
                                             logAbortLsn,
					     logAbortKnownDeleted,
                                             logTxn);

        LogManager logManager = env.getLogManager();
        return logManager.log(logEntry, isProvisional, oldLsn, isDeleted());
    }

    /**
     * Log type for transactional entries
     */
    protected LogEntryType getTransactionalLogType() {
        return LogEntryType.LOG_LN_TRANSACTIONAL;
    }

    /**
     * @see LoggableObject#getLogType
     */
    public LogEntryType getLogType() {
        return LogEntryType.LOG_LN;
    }

    /**
     * @see LoggableObject#getLogSize
     */
    public int getLogSize() {
        int size = super.getLogSize();

        // data
        size += LogUtils.getBooleanLogSize(); // isDeleted flag
        if (!isDeleted()) {
            size += LogUtils.getByteArrayLogSize(data);
        }

        return size;
    }

    /**
     * @see LoggableObject#writeToLog
     */
    public void writeToLog(ByteBuffer logBuffer) {
        // Ask ancestors to write to log
        super.writeToLog(logBuffer); 

        // data: isData null flag, then length, then data
        boolean dataExists = !isDeleted();
        LogUtils.writeBoolean(logBuffer, dataExists);
        if (dataExists) {
            LogUtils.writeByteArray(logBuffer, data);
        }
    }

    /**
     * @see LogReadable#readFromLog
     */
    public void readFromLog(ByteBuffer itemBuffer)
        throws LogException {

        super.readFromLog(itemBuffer);

        boolean dataExists = LogUtils.readBoolean(itemBuffer);
        if (dataExists) {
            data = LogUtils.readByteArray(itemBuffer);
        }
    }

    /**
     * @see LogReadable#dumpLog
     */
    public void dumpLog(StringBuffer sb, boolean verbose) {
        sb.append(beginTag());
        super.dumpLog(sb, verbose);

        if (data != null) {
            sb.append("<data>");
            sb.append(TreeUtils.dumpByteArray(data));
            sb.append("</data>");
        }
        
        dumpLogAdditional(sb);

        sb.append(endTag());
    }

    /**
     * Never called.
     * @see LogReadable#logEntryIsTransactional.
     */
    public boolean logEntryIsTransactional() {
	return false;
    }

    /**
     * Never called.
     * @see LogReadable#getTransactionId
     */
    public long getTransactionId() {
	return 0;
    }

    /*
     * Allows subclasses to add additional fields before the end tag.
     */
    protected void dumpLogAdditional(StringBuffer sb) {
    }
}
