/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: DatabaseImpl.java,v 1.1 2004/11/22 18:27:59 kate Exp $
*/

package com.sleepycat.je.dbi;

import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;

import com.sleepycat.je.BtreeStats;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DatabaseStats;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.VerifyConfig;
import com.sleepycat.je.cleaner.TrackedFileSummary;
import com.sleepycat.je.cleaner.UtilizationTracker;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.latch.Latch;
import com.sleepycat.je.log.LogException;
import com.sleepycat.je.log.LogReadable;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.log.LogWritable;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.DBIN;
import com.sleepycat.je.tree.DIN;
import com.sleepycat.je.tree.DupCountLN;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.tree.Node;
import com.sleepycat.je.tree.Tree;
import com.sleepycat.je.tree.TreeUtils;
import com.sleepycat.je.tree.TreeWalkerStatsAccumulator;
import com.sleepycat.je.txn.Locker;
import com.sleepycat.je.txn.ThreadLocker;
import com.sleepycat.je.utilint.DbLsn;

/**
 * The underlying object for a given database.
 */
public class DatabaseImpl
    implements LogWritable, LogReadable, Cloneable {

    private DatabaseId id;        // unique id
    private Tree tree;             
    private EnvironmentImpl envImpl;  // Tree operations find the env this way
    private boolean duplicatesAllowed; // duplicates allowed
    private boolean transactional; // All open handles are transactional
    private Set referringHandles; // Set of open Database handles    
    private boolean isDeleted;    // DatabaseImpl has been deleted--do not use.
    private TrackedFileSummary[] deletedTrackingInfo; // Used during delete
    private BtreeStats stats;  // most recent btree stats w/ !DB_FAST_STAT

    /*
     * The user defined Btree and duplicate comparison functions, if specified.
     */
    private Comparator btreeComparator = null;
    private Comparator duplicateComparator = null;

    /*
     * Cache some configuration values.
     */
    private int binDeltaPercent;
    private int binMaxDeltas;

    /**
     * Create a database object for a new database.
     */
    public DatabaseImpl(String dbName,
			DatabaseId id,
			EnvironmentImpl envImpl,
			DatabaseConfig dbConfig)
        throws DatabaseException {

        this.id = id;
        this.envImpl = envImpl;
        this.btreeComparator = dbConfig.getBtreeComparator();
        this.duplicateComparator = dbConfig.getDuplicateComparator();
        duplicatesAllowed = dbConfig.getSortedDuplicates();
        transactional = dbConfig.getTransactional();

        isDeleted = false;

        /*
         * The tree needs the env, make sure we assign it before
         * allocating the tree.
         */
        tree = new Tree(this); 
        referringHandles = Collections.synchronizedSet(new HashSet());
        DbConfigManager configMgr = envImpl.getConfigManager();
        binDeltaPercent =
            configMgr.getInt(EnvironmentParams.BIN_DELTA_PERCENT);
        binMaxDeltas =
            configMgr.getInt(EnvironmentParams.BIN_MAX_DELTAS);
    }

    /**
     * Create an empty database object for initialization from the log.  Note
     * that the rest of the initialization comes from readFromLog().
     */ 
    public DatabaseImpl()
        throws DatabaseException {

        id = new DatabaseId();
        envImpl = null;

        isDeleted = false;
        tree = new Tree();
        referringHandles = Collections.synchronizedSet(new HashSet());
        /* binDeltaPercent, binMaxDeltas get initialized after env is set. */
    }

    /**
     * Clone.  For now just pass off to the super class for a field-by-field
     * copy.
     */
    public Object clone()
        throws CloneNotSupportedException {

        return super.clone();
    }

    /**
     * @return the database tree.
     */
    public Tree getTree() {
        return tree;
    }

    void setTree(Tree tree) {
        this.tree = tree;
    }

    /**
     * @return the database id.
     */
    public DatabaseId getId() {
        return id;
    }

    void setId(DatabaseId id) {
        this.id = id;
    }

    /**
     * @return true if this database is transactional.
     */
    public boolean isTransactional() {
        return transactional;
    }

    /**
     * Sets the transactional property for the first opened handle.
     */
    public void setTransactional(boolean transactional) {
        this.transactional = transactional;
    }

    /**
     * @return true if duplicates are allowed in this database.
     */
    public boolean getSortedDuplicates() {
        return duplicatesAllowed;
    }

    /**
     * Set the duplicate comparison function for this database.
     *
     * @param duplicateComparator - The Duplicate Comparison function.
     */
    public void setDuplicateComparator(Comparator duplicateComparator) {
        this.duplicateComparator = duplicateComparator;
    }

    /**
     * Set the btree comparison function for this database.
     *
     * @param btreeComparator - The btree Comparison function.
     */
    public void setBtreeComparator(Comparator btreeComparator) {
        this.btreeComparator = btreeComparator;
    }

    /**
     * @return the btree Comparator object.
     */
    public Comparator getBtreeComparator() {
        return btreeComparator;
    }

    /**
     * @return the duplicate Comparator object.
     */
    public Comparator getDuplicateComparator() {
        return duplicateComparator;
    }

    /**
     * Set the db environment during recovery, after instantiating the database
     * from the log
     */
    public void setEnvironmentImpl(EnvironmentImpl envImpl)
        throws DatabaseException{

        this.envImpl = envImpl;
        binDeltaPercent =
            envImpl.getConfigManager().
              getInt(EnvironmentParams.BIN_DELTA_PERCENT);
        binMaxDeltas =
            envImpl.getConfigManager().
              getInt(EnvironmentParams.BIN_MAX_DELTAS);
        tree.setDatabase(this);
    }

    /**
     * @return the database environment.
     */
    public EnvironmentImpl getDbEnvironment() {
        return envImpl;
    }

    /**
     * Returns whether one or more handles are open.
     */
    public boolean hasOpenHandles() {
        return referringHandles.size() > 0;
    }

    /**
     * Add a referring handle
     */
    public void addReferringHandle(Database db) {
        referringHandles.add(db);
    }

    /**
     * Decrement the reference count.
     */
    public void removeReferringHandle(Database db) {
        referringHandles.remove(db);
    }

    /**
     * @return the referring handle count.
     */
    synchronized int getReferringHandleCount() {
        return referringHandles.size();
    }

    public String getName() 
        throws DatabaseException {

        return envImpl.getDbMapTree().getDbName(id);
    }

    public boolean getIsDeleted() {
        return isDeleted;
    }

    public void deleteAndReleaseINs() 
        throws DatabaseException {

        this.isDeleted = true;
        envImpl.getInMemoryINs().clearDb(this);

        /*
         * Count obsolete nodes for a deleted database at transaction commit
         * time.  This calls the LogManager which does the counting under the
         * log write latch.
         */
        envImpl.getLogManager().countObsoleteNodes(deletedTrackingInfo);
        deletedTrackingInfo = null;
    }

    public void checkIsDeleted(String operation)
        throws DatabaseException {

        if (isDeleted) {
            throw new DatabaseException
                ("Attempt to " + operation + " a deleted database");
        }
    }
    
    /**
     * Called when this database is truncated or removed to record the number
     * of obsolete nodes that should be counted for the deleted tree.  We save
     * the number of obsolete nodes here, but the counting in the utilization
     * profile does not actually occur until deleteAndReleaseINs() is called
     * when the transaction is committed.
     */
    public int recordObsoleteNodes()
        throws DatabaseException {

        /*
         * Needs improvement: This is very inefficient since we load every
         * database record and copy its data, and then discard the data.
         * Perhaps internal cursor methods should allow passing null for the
         * DatabaseEntry parameters, since parameter checking is done by the
         * public API.
         */
        UtilizationTracker tracker = new UtilizationTracker(envImpl);
        Locker locker = new ThreadLocker(envImpl);
        Cursor cursor = null;
        int count = 0;
        try {
            cursor = DbInternal.newCursor(this, locker, null);
            DatabaseEntry foundData = new DatabaseEntry();
            DatabaseEntry key = new DatabaseEntry();
            OperationStatus status = DbInternal.position(cursor, key,
                                                         foundData,
                                                         LockMode.DIRTY_READ,
                                                         true);
            while (status == OperationStatus.SUCCESS) {
                count++;
                CursorImpl impl = DbInternal.getCursorImpl(cursor);
                DbLsn lsn = impl.getBIN().getEntry(impl.getIndex()).getLsn();
                tracker.countObsoleteNode(lsn, null, true);
                status = DbInternal.retrieveNext(cursor, key, foundData,
                                                 LockMode.DIRTY_READ,
                                                 GetMode.NEXT);
            } 
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        deletedTrackingInfo = tracker.getTrackedFiles();
        return count;
    }

    public DatabaseStats stat(StatsConfig config) 
        throws DatabaseException {

        if (!config.getFast()) {
            if (tree == null) {
                return new BtreeStats();
            }
	    StatsAccumulator statsAcc =
		new StatsAccumulator(config.getShowProgressStream(),
				     config.getShowProgressInterval());
	    walkDatabaseTree(statsAcc);
            stats = statsAcc.copyToBtreeStats();
        }

        if (stats == null) {

            /* 
             * Called first time w/ FAST_STATS so just give them an
             * empty one.
             */
            stats = new BtreeStats();
        }

        return stats;
    }

    public DatabaseStats verify(VerifyConfig config) 
        throws DatabaseException {

	if (tree == null) {
	    return new BtreeStats();
	}

	PrintStream out = config.getShowProgressStream();
	if (out == null) {
	    out = System.err;
	}

	StatsAccumulator statsAcc =
	    new StatsAccumulator(out, config.getShowProgressInterval()) {
		    void verifyNode(Node node) {

			try {
			    node.verify(null);
			} catch (DatabaseException INE) {
			    progressStream.println(INE);
			}
		    }
		};
	walkDatabaseTree(statsAcc);
	return statsAcc.copyToBtreeStats();
    }

    public void walkDatabaseTree(TreeWalkerStatsAccumulator statsAcc)
        throws DatabaseException {

        Locker locker = new ThreadLocker(envImpl);
        Cursor cursor = null;
        try {
            cursor = DbInternal.newCursor(this, locker, null);
	    CursorImpl impl = DbInternal.getCursorImpl(cursor);
	    tree.setTreeStatsAccumulator(statsAcc);
	    /* 
	     * This will only be used on the first call for the position()
	     * call.
	     */
	    impl.setTreeStatsAccumulator(statsAcc);
            DatabaseEntry foundData = new DatabaseEntry();
            DatabaseEntry key = new DatabaseEntry();
            OperationStatus status =
		DbInternal.position(cursor, key, foundData,
				    LockMode.DIRTY_READ, true);
	    int count = 0;
            while (status == OperationStatus.SUCCESS) {
                status = DbInternal.retrieveNext(cursor, key, foundData,
                                                 LockMode.DIRTY_READ,
                                                 GetMode.NEXT);
            }

        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
    }

    static class StatsAccumulator implements TreeWalkerStatsAccumulator {
	private Set inNodeIdsSeen = new HashSet();
	private Set binNodeIdsSeen = new HashSet();
	private Set dinNodeIdsSeen = new HashSet();
	private Set dbinNodeIdsSeen = new HashSet();
	private Set dupCountLNsSeen = new HashSet();
	private long[] insSeenByLevel = null;
	private long[] binsSeenByLevel = null;
	private long[] dinsSeenByLevel = null;
	private long[] dbinsSeenByLevel = null;
	private long lnCount = 0;
	private long deletedLNCount = 0;
	private int mainTreeMaxDepth = 0;
	private int duplicateTreeMaxDepth = 0;

	PrintStream progressStream;
	int progressInterval;

	/* The max levels we ever expect to see in a tree. */
	private static final int MAX_LEVELS = 100;

	StatsAccumulator(PrintStream progressStream,
			 int progressInterval) {

	    this.progressStream = progressStream;
	    this.progressInterval = progressInterval;

	    insSeenByLevel = new long[MAX_LEVELS];
	    binsSeenByLevel = new long[MAX_LEVELS];
	    dinsSeenByLevel = new long[MAX_LEVELS];
	    dbinsSeenByLevel = new long[MAX_LEVELS];
	}

	void verifyNode(Node node) {

	}

	public void processIN(IN node, Long nid, int level) {
	    if (inNodeIdsSeen.add(nid)) {
		tallyLevel(level, insSeenByLevel);
		verifyNode(node);
	    }
	}

	public void processBIN(BIN node, Long nid, int level) {
	    if (binNodeIdsSeen.add(nid)) {
		tallyLevel(level, binsSeenByLevel);
		verifyNode(node);
	    }
	}

	public void processDIN(DIN node, Long nid, int level) {
	    if (dinNodeIdsSeen.add(nid)) {
		tallyLevel(level, dinsSeenByLevel);
		verifyNode(node);
	    }
	}

	public void processDBIN(DBIN node, Long nid, int level) {
	    if (dbinNodeIdsSeen.add(nid)) {
		tallyLevel(level, dbinsSeenByLevel);
		verifyNode(node);
	    }
	}

	public void processDupCountLN(DupCountLN node, Long nid) {
	    dupCountLNsSeen.add(nid);
	    verifyNode(node);
	}

	private void tallyLevel(int levelArg, long[] nodesSeenByLevel) {
	    int level = levelArg;
	    if (level >= IN.DBMAP_LEVEL) {
		return;
	    }
	    if (level >= IN.MAIN_LEVEL) {
		level &= ~IN.MAIN_LEVEL;
		if (level > mainTreeMaxDepth) {
		    mainTreeMaxDepth = level;
		}
	    } else {
		if (level > duplicateTreeMaxDepth) {
		    duplicateTreeMaxDepth = level;
		}
	    }

	    nodesSeenByLevel[level]++;
	}

	public void incrementLNCount() {
	    lnCount++;
	    if (progressInterval != 0) {
		if ((lnCount % progressInterval) == 0) {
		    progressStream.println(copyToBtreeStats());
		}
	    }
	}

	public void incrementDeletedLNCount() {
	    deletedLNCount++;
	}

	Set getINNodeIdsSeen() {
	    return inNodeIdsSeen;
	}

	Set getBINNodeIdsSeen() {
	    return binNodeIdsSeen;
	}

	Set getDINNodeIdsSeen() {
	    return dinNodeIdsSeen;
	}

	Set getDBINNodeIdsSeen() {
	    return dbinNodeIdsSeen;
	}

	long[] getINsByLevel() {
	    return insSeenByLevel;
	}

	long[] getBINsByLevel() {
	    return binsSeenByLevel;
	}

	long[] getDINsByLevel() {
	    return dinsSeenByLevel;
	}

	long[] getDBINsByLevel() {
	    return dbinsSeenByLevel;
	}

	long getLNCount() {
	    return lnCount;
	}

	Set getDupCountLNCount() {
	    return dupCountLNsSeen;
	}

	long getDeletedLNCount() {
	    return deletedLNCount;
	}

	int getMainTreeMaxDepth() {
	    return mainTreeMaxDepth;
	}

	int getDuplicateTreeMaxDepth() {
	    return duplicateTreeMaxDepth;
	}

	private BtreeStats copyToBtreeStats() {
	    BtreeStats ret = new BtreeStats();
	    ret.setInternalNodeCount(getINNodeIdsSeen().size());
	    ret.setBottomInternalNodeCount
		(getBINNodeIdsSeen().size());
	    ret.setDuplicateInternalNodeCount
		(getDINNodeIdsSeen().size());
	    ret.setDuplicateBottomInternalNodeCount
		(getDBINNodeIdsSeen().size());
	    ret.setLeafNodeCount(getLNCount());
	    ret.setDeletedLeafNodeCount(getDeletedLNCount());
	    ret.setDupCountLeafNodeCount
		(getDupCountLNCount().size());
	    ret.setMainTreeMaxDepth(getMainTreeMaxDepth());
	    ret.setDuplicateTreeMaxDepth(getDuplicateTreeMaxDepth());
	    ret.setINsByLevel(getINsByLevel());
	    ret.setBINsByLevel(getBINsByLevel());
	    ret.setDINsByLevel(getDINsByLevel());
	    ret.setDBINsByLevel(getDBINsByLevel());
	    return ret;
	}
    }

    /**
     * Preload the cache, using up to maxBytes bytes. 
     */
    public void preload(long maxBytes)
	throws DatabaseException {

	IN next = tree.getFirstNode();
	if (next == null) {
	    return;
	}
	next.releaseLatch();

	if (maxBytes == 0) {
            maxBytes = envImpl.getMemoryBudget().getTreeBudget();
	}

	while (next != null) {
	    next = tree.getNextBin((BIN) next, null);
	    if (next == null) {
		break;
	    }
	    next.releaseLatch();

	    if (envImpl.getMemoryBudget().getCacheMemoryUsage() >
                maxBytes) {
		break;
	    }
	}

        assert Latch.countLatchesHeld() == 0;
    }

    /*
     * Dumping
     */
    public String dumpString(int nSpaces) {
        StringBuffer sb = new StringBuffer();
        sb.append(TreeUtils.indent(nSpaces));
        sb.append("<database id=\"" );
        sb.append(id.toString());
        sb.append("\"");
        if (btreeComparator != null) {
            sb.append(" btc=\"");
            sb.append(serializeComparator(btreeComparator));
            sb.append("\"");
        }
        if (duplicateComparator != null) {
            sb.append(" dupc=\"");
            sb.append(serializeComparator(duplicateComparator));
            sb.append("\"");
        }
        sb.append("/>");
        return sb.toString();
    }

    /*
     * Logging support
     */

    /**
     * @see LogWritable#getLogSize
     */
    public int getLogSize() {
        return 
            id.getLogSize() +
            tree.getLogSize() +
            LogUtils.getBooleanLogSize() +
            LogUtils.getStringLogSize(serializeComparator(btreeComparator)) +
            LogUtils.getStringLogSize
            (serializeComparator(duplicateComparator));
    }

    /**
     * @see LogWritable#writeToLog
     */
    public void writeToLog(ByteBuffer logBuffer) {
        id.writeToLog(logBuffer);
        tree.writeToLog(logBuffer);
        LogUtils.writeBoolean(logBuffer,
			      duplicatesAllowed);
        LogUtils.writeString(logBuffer,
			     serializeComparator(btreeComparator));
        LogUtils.writeString(logBuffer,
			     serializeComparator(duplicateComparator));
    }

    /**
     * @see LogReadable#readFromLog
     */
    public void readFromLog(ByteBuffer itemBuffer)
        throws LogException {

        id.readFromLog(itemBuffer);
        tree.readFromLog(itemBuffer);
        duplicatesAllowed = LogUtils.readBoolean(itemBuffer);

        String btreeComparatorName = LogUtils.readString(itemBuffer);
        String duplicateComparatorName = LogUtils.readString(itemBuffer);

	try {
	    if (btreeComparatorName.length() != 0) {
		Class btreeComparatorClass =
		    Class.forName(btreeComparatorName);
		btreeComparator =
		    instantiateComparator(btreeComparatorClass, "Btree");
	    }
	    if (duplicateComparatorName.length() != 0) {
		Class duplicateComparatorClass =
		    Class.forName(duplicateComparatorName);
		duplicateComparator =
		    instantiateComparator(duplicateComparatorClass,
					  "Duplicate");
	    }
	} catch (ClassNotFoundException CNFE) {
	    throw new LogException("couldn't instantiate class comparator",
				   CNFE);
	}
    }

    /**
     * @see LogReadable#dumpLog
     */
    public void dumpLog(StringBuffer sb, boolean verbose) {
        sb.append("<database>");
        id.dumpLog(sb, verbose);
        tree.dumpLog(sb, verbose);
        sb.append("<dupsort v=\"").append(duplicatesAllowed);
        sb.append("\"/>");
        sb.append("<btcf name=\"");
        sb.append(serializeComparator(btreeComparator));
        sb.append("\"/>");
        sb.append("<dupcf name=\"");
        sb.append(serializeComparator(duplicateComparator));
        sb.append("\"/>");
        sb.append("</database>");
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

    /**
     * Used both to write to the log and to validate a comparator when set in 
     * DatabaseConfig.
     */
    public static String serializeComparator(Comparator comparator) {
        if (comparator != null) {
            return comparator.getClass().getName();
        } else {
            return "";
        }
    }

    /**
     * Used both to read from the log and to validate a comparator when set in 
     * DatabaseConfig.
     */
    public static Comparator instantiateComparator(Class comparator,
                                                   String comparatorType) 
        throws LogException {

	if (comparator == null) {
	    return null;
	}

        Comparator ret = null;
        try {
	    return (Comparator) comparator.newInstance();
        } catch (InstantiationException IE) {
            throw new LogException
                ("Exception while trying to load " + comparatorType +
                 " Comparator class: " + IE);
        } catch (IllegalAccessException IAE) {
            throw new LogException
                ("Exception while trying to load " + comparatorType +
                 " Comparator class: " + IAE);
        }
    }

    public int getBinDeltaPercent() {
        return binDeltaPercent;
    }

    public int getBinMaxDeltas() {
        return binMaxDeltas;
    }
}
