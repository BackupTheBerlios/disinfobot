/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: ChildReference.java,v 1.1 2004/11/22 18:27:57 kate Exp $
*/

package com.sleepycat.je.tree;

import java.nio.ByteBuffer;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.MemoryBudget;
import com.sleepycat.je.log.LogFileNotFoundException;
import com.sleepycat.je.log.LogReadable;
import com.sleepycat.je.log.LogWritable;
import com.sleepycat.je.utilint.DbLsn;

/**
 * A ChildReference is a reference in the tree from parent to child.  It
 * contains a node reference, key, and lsn.
 */
public final class ChildReference implements LogWritable, LogReadable {
    private Node target;
    private DbLsn lsn;
    private Key key;

    /*
     * The state byte holds knownDeleted state in bit 0 and dirty state in bit
     * 1. Bit flags are used here because of the desire to keep the
     * child reference compact. State is persistent because knownDeleted is
     * persistent, but the dirty bit is cleared when read in from the log.
     *  
     * -- KnownDeleted is a way of indicating that the reference is 
     * invalid without logging new data. This happens in aborts and
     * recoveries. If knownDeleted is true, this entry is surely deleted. If
     * knownDeleted is false, this entry may or may not be
     * deleted. Future space optimizations: store as a separate bit array
     * in the BIN, or  subclass ChildReference and make a special reference
     * only used by BINs and not by INs.
     *
     * -- Dirty is true if the lsn or key has been changed since the
     * last time the ownign node was logged. This supports the calculation 
     * of BIN deltas.
     */
    private byte state;
    private static final byte KNOWN_DELETED_BIT = 0x1;
    private static final byte CLEAR_KNOWN_DELETED_BIT = ~0x1;
    private static final byte DIRTY_BIT = 0x2;
    private static final byte CLEAR_DIRTY_BIT = ~0x2;

    /**
     * Construct an empty child reference, for reading from the log
     */
    ChildReference() {
        init(null, new Key(), new DbLsn(), false, false);
    }

    /**
     * Construct a ChildReference.
     */
    public ChildReference(Node target, Key key, DbLsn lsn) {
        init(target, key, lsn, false, true);
    }

    /**
     * Construct a ChildReference.
     */
    ChildReference(Key key, DbLsn lsn, boolean knownDeleted) {
        init(null, key, lsn, knownDeleted, true);
    }

    private void init(Node target, Key key, DbLsn lsn,
                      boolean knownDeleted, boolean dirty) {
        this.target = target;
        this.key = key;
        this.lsn = lsn;
        state = 0;
        if (knownDeleted) {
            state |= KNOWN_DELETED_BIT;
        }
        if (dirty) {
            state |= DIRTY_BIT;
        }
    }

    /**
     * Return the key for this ChildReference.
     */
    public Key getKey() {
        return key;
    }

    /**
     * Set the key for this ChildReference.
     */
    public void setKey(Key key) {
        this.key = key;
        state |= DIRTY_BIT;
    }

    /**
     * Fetch the target object that this ChildReference refers to.  If
     * the object is already in VM, then just return the reference to
     * it.  If the object is not in VM, then read the object from the log.
     * If the object has been faulted in and  the in arg is supplied, then 
     * the total memory size cache in the IN is invalidated.
     *
     * @param database The database that this ChildReference resides in.
     * @param in The IN that this ChildReference lives in.  If
     * the target is fetched (i.e. it is null on entry), then the
     * total in memory count is invalidated in the IN. May be null. 
     * For example, the root is a ChildReference and there is no parent IN
     * when the rootIN is fetched in.
     * @return the Node object representing the target node in the tree or
     * null if there is no target of this ChildReference.
     */    
    public Node fetchTarget(DatabaseImpl database, IN in)
        throws DatabaseException {

	if (isKnownDeleted()) {
	    throw new DatabaseException("attempt to fetch a deleted entry: " +
					lsn);
	}
	return fetchTargetInternal(database, in);
    }

    /**
     * Same as fetchTarget except that if knownDeleted is set this does
     * not thrown an exception.  This is needed by the compressor to
     * fetch an LN so that it can lock it, even though it's marked as deleted.
     */
    Node fetchTargetIgnoreKnownDeleted(DatabaseImpl database, IN in)
        throws DatabaseException {

	return fetchTargetInternal(database, in);
    }

    private Node fetchTargetInternal(DatabaseImpl database, IN in)
        throws DatabaseException {

        if (target == null) {
            /* fault object in from log */
            try {
                EnvironmentImpl env = database.getDbEnvironment();
                Node node = (Node) env.getLogManager().get(lsn);
                node.postFetchInit(database);
                target = node;
	    } catch (LogFileNotFoundException LNFE) {
		if (!isKnownDeleted()) {
                    throw new DatabaseException(makeFetchErrorMsg(in, LNFE),
                                                LNFE);
		}
		/* Ignore. Cleaner got to the log file, so just return null. */
            } catch (Exception e) {
                throw new DatabaseException(makeFetchErrorMsg(in, e), e);
            }
	    if (in != null) {
		in.updateMemorySize(null, target);
	    }
        }

        return target;
    }

    private String makeFetchErrorMsg(IN parent, Exception e) {

        /*
         * Bolster the exception with the lsn, which is
         * critical for debugging. Otherwise, the exception
         * propagates upward and loses the problem lsn.
         */
        StringBuffer sb = new StringBuffer();
        sb.append("fetchTarget of ");
        if (lsn == null) {
            sb.append("null lsn");
        } else {
            sb.append(lsn.getNoFormatString());
        }
        if (parent != null) {
            sb.append(" parent in=").append(parent.getNodeId());
        }
        sb.append(" state=").append(state);
        sb.append(" ").append(e);
        return sb.toString();
    }

    /**
     * Return the target for this ChildReference.
     */
    public Node getTarget() {
        return target;
    }

    /**
     * Sets the target for this ChildReference. No need to make dirty,
     * that state only applies to key and lsn.
     */
    public void setTarget(Node target) {
        this.target = target;
    }

    /**
     * Clear the target for this ChildReference. No need to make dirty,
     * that state only applies to key and lsn. This method is public because
     * it's safe and used by RecoveryManager. This can't corrupt the tree.
     */
    public void clearTarget() {
        this.target = null;
    }

    /**
     * Return the Lsn for this ChildReference.
     *
     * @return the Lsn for this ChildReference.
     */
    public DbLsn getLsn() {
        return lsn;
    }

    /**
     * Sets the target Lsn for this ChildReference.
     *
     * @param the target Lsn.
     */
    public void setLsn(DbLsn lsn) {
        this.lsn = lsn;
        state |= DIRTY_BIT;
    }

    /**
     * @return true if entry is deleted for sure.
     */
    public boolean isKnownDeleted() {
        return ((state & KNOWN_DELETED_BIT) != 0);
    }

    /**
     * Set knownDeleted to true
     */
    void setKnownDeleted() {
        state |= KNOWN_DELETED_BIT;
        state |= DIRTY_BIT;
    }

    /**
     * Set knownDeleted to false
     */
    void clearKnownDeleted() {
        state &= CLEAR_KNOWN_DELETED_BIT;
        state |= DIRTY_BIT;
    }

    /**
     * @return true if the object is dirty.
     */
    boolean isDirty() {
        return ((state & DIRTY_BIT) != 0);
    }

    long getInMemorySize() {
	long ret = 1;                     // 1 byte for state field
	if (key != null) {
	    ret += key.getKey().length + MemoryBudget.KEY_OVERHEAD;
	}
	if (lsn != null) {
	    ret += MemoryBudget.LSN_SIZE;
	}
	if (target != null) {
	    ret += target.getMemorySizeIncludedByParent();
	}
	return ret;
    }

    /* 
     * Support for logging.
     */

    /**
     * @see LogWritable#getLogSize
     */
    public int getLogSize() {
        return
            key.getLogSize() +             // key
            lsn.getLogSize() +             // lsn
            1;                             // state
    }

    /**
     * @see LogWritable#writeToLog
     */
    public void writeToLog(ByteBuffer logBuffer) {
        key.writeToLog(logBuffer);  // key
        assert lsn != null;
        lsn.writeToLog(logBuffer);  // lsn
        logBuffer.put(state);       // state
        state &= CLEAR_DIRTY_BIT;
    }

    /**
     * @see LogReadable#readFromLog
     */
    public void readFromLog(ByteBuffer itemBuffer) {
        key.readFromLog(itemBuffer);       // key
        lsn.readFromLog(itemBuffer);       // lsn
        state = itemBuffer.get();          // state
        state &= CLEAR_DIRTY_BIT;
    }

    /**
     * @see LogReadable#dumpLog
     */
    public void dumpLog(StringBuffer sb, boolean verbose) {
        sb.append("<ref knownDeleted=\"").append(isKnownDeleted());
        sb.append("\">");
        key.dumpLog(sb, verbose);
        lsn.dumpLog(sb, verbose);
        sb.append("</ref>");
    }   

    /**
     * @see LogReadable#logEntryIsTransactional
     */
    public boolean logEntryIsTransactional() {
	return false;
    }

    /**
     * @see LogReadable#getTransactionId
     */
    public long getTransactionId() {
	return 0;
    }

    /*
     * Dumping
     */
    String dumpString(int nspaces, boolean dumpTags) {
        StringBuffer sb = new StringBuffer();
        if (lsn == null) {
            sb.append(TreeUtils.indent(nspaces));
            sb.append("<lsn/>");
        } else {
            sb.append(lsn.dumpString(nspaces));
        }
        sb.append('\n');
        if (key == null) {
            sb.append(TreeUtils.indent(nspaces));
            sb.append("<key/>");
        } else {
            sb.append(key.dumpString(nspaces));
        }
        sb.append('\n');
        if (target == null) {
            sb.append(TreeUtils.indent(nspaces));
            sb.append("<target/>");
        } else {
            sb.append(target.dumpString(nspaces, true));
        }
        sb.append('\n');
        sb.append(TreeUtils.indent(nspaces));
        sb.append("<knownDeleted val=\"");
        sb.append(isKnownDeleted()).append("\"/>");
        sb.append("<dirty val=\"").append(isDirty()).append("\"/>");
        return sb.toString();
    }

    public String toString() {
        return dumpString(0, false);
    }
}
