/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: IN.java,v 1.1 2004/11/22 18:27:57 kate Exp $
*/

package com.sleepycat.je.tree;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.DbTree;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.INList;
import com.sleepycat.je.dbi.MemoryBudget;
import com.sleepycat.je.latch.Latch;
import com.sleepycat.je.latch.LatchNotHeldException;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogException;
import com.sleepycat.je.log.LogManager;
import com.sleepycat.je.log.LogReadable;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.log.LoggableObject;
import com.sleepycat.je.log.entry.INLogEntry;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.Tracer;

/**
 * An IN represents an Internal Node in the JE tree.
 */
public class IN extends Node
    implements Comparable, LoggableObject, LogReadable {

    private static final String BEGIN_TAG = "<in>";
    private static final String END_TAG = "</in>";
    private static final String TRACE_SPLIT = "Split:";
    private static final String TRACE_DELETE = "Delete:";

    /* 
     * Levels: 
     * The mapping tree has levels in the 0x20000 -> 0x2ffffnumber space.
     * The main tree has levels in the 0x10000 -> 0x1ffff number space.
     * The duplicate tree levels are in 0-> 0xffff number space.
     */
    public static final int DBMAP_LEVEL = 0x20000;
    public static final int MAIN_LEVEL = 0x10000;

    private Latch latch;
    private long generation;
    private boolean dirty;
    private int nEntries;
    private Key identifierKey;
    private ChildReference[] entries;
    private DatabaseImpl databaseImpl;
    private boolean isRoot; // true if this is the root of a tree
    private int level;
    private long inMemorySize;
    private boolean inListResident; // true if this IN is on the IN list

    /* 
     * If true, this node cannot be evicted. Used to hold a node resident
     * across an unlatched period. For example, when cleaning, an IN
     * may be unlatched in order to get a dup count ln lock. We need
     * to keep the IN resident across the period when we obtained the lock
     * so that any updates are done on the correct object.
     */
    protected boolean evictionProhibited;

    /* Used to indicate that an exact match was found in findEntry. */
    public static final int EXACT_MATCH = (1 << 16);

    /* Used to indicate that an insert was successful. */
    public static final int INSERT_SUCCESS = (1 << 17);

    /**
     * Create an empty IN, with no node id, to be filled in from the log.
     */
    public IN() {
        super(false); 
        init(null, new Key(), 0, 0);
    }

    /**
     * Create a new IN.
     */
    public IN(DatabaseImpl db, Key identifierKey, int capacity, int level) {

        super(true);
        init(db, identifierKey, capacity, generateLevel(db.getId(), level));
        initMemorySize();
    }

    /**
     * Initialize IN object.
     */
    protected void init(DatabaseImpl db, Key identifierKey,
                        int initialCapacity, int level) {
        setDatabase(db);
	EnvironmentImpl env =
	    (databaseImpl == null) ? null : databaseImpl.getDbEnvironment();
        latch = new Latch(shortClassName() + getNodeId(), env);
        generation = 0;
        dirty = false;
        nEntries = 0;
        this.identifierKey = identifierKey;
        entries = new ChildReference[initialCapacity];
        isRoot = false;
        this.level = level;
        inListResident = false;
        evictionProhibited = false;
    }

    /**
     * Initialize the per-node memory count by computing its memory usage.
     */
    protected void initMemorySize() {
        inMemorySize = computeMemorySize();
    }

    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (!(obj instanceof IN)) {
            return false;
        }

        IN in = (IN) obj;
        return (in.getNodeId() == getNodeId());
    }

    public int hashCode() {
        return (int) (getNodeId() & 0xffffffff);
    }

    /**
     * Sort based on node id.
     */
    public int compareTo(Object o) {
        if (o == null) {
            throw new NullPointerException();
        }

        IN argIN = (IN) o;

        long argNodeId = argIN.getNodeId();
        long myNodeId = getNodeId();
        if (myNodeId < argNodeId) {
            return -1;
        } else if (myNodeId > argNodeId) {
            return 1;
        } else {
            return 0;
        }
    }

    /**
     * Create a new IN.  Need this because we can't call newInstance()
     * without getting a 0 for nodeid.
     */
    protected IN createNewInstance(Key identifierKey,
                                   int maxEntries,
                                   int level) {
        return new IN(databaseImpl, identifierKey, maxEntries, level);
    }

    /**
     * Initialize a node that has been read in from the log.
     */
    public void postFetchInit(DatabaseImpl db)
        throws DatabaseException {

        setDatabase(db);
        EnvironmentImpl env = db.getDbEnvironment();
        initMemorySize(); // compute before adding to inlist
        env.getInMemoryINs().add(this);
    }

    /**
     * Initialize a node read in during recovery.
     */
    public void postRecoveryInit(DatabaseImpl db, DbLsn sourceLsn) {
        setDatabase(db);
        setLastFullLsn(sourceLsn);
        initMemorySize();
    }

    /**
     * For nodes that must remember their source lsn. INs don't need to
     * remember.
     */
    protected void setLastFullLsn(DbLsn lsn) {
    }

    /**
     * Latch this node.
     */
    public void latch()
        throws DatabaseException {

        setGeneration();
        latch.acquire();
    }
    
    /**
     * Release the latch on this node.
     */
    public void releaseLatch()
        throws LatchNotHeldException {

        latch.release();
    }

    /**
     * @return the latch object for this node.
     */
    public Latch getLatch() {
        return latch;
    }

    public long getGeneration() {
        return generation;
    }

    public void setGeneration() {
        generation = Generation.getNextGeneration();
    }

    public void setGeneration(long newGeneration) {
        generation = newGeneration;
    }

    public int getLevel() {
        return level;
    }

    protected int generateLevel(DatabaseId dbId, int newLevel) {
        if (dbId.equals(DbTree.ID_DB_ID)) {
            return newLevel | DBMAP_LEVEL;
        } else {
            return newLevel | MAIN_LEVEL;
        }
    }

    public boolean getDirty() {
        return dirty;
    }

    /* public for unit tests */
    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    public boolean isRoot() {
        return isRoot;
    }

    public boolean isDbRoot() {
	return isRoot;
    }

    void setIsRoot(boolean isRoot) {
        this.isRoot = isRoot;
        setDirty(true);
    }

    /**
     * @return the identifier key for this node.
     */
    public Key getIdentifierKey() {
        return identifierKey;
    }

    /**
     * Set the identifier key for this node.
     *
     * @param key - the new identifier key for this node.
     */
    void setIdentifierKey(Key key) {
        identifierKey = key;
        setDirty(true);
    }

    /**
     * Get the key (dupe or identifier) in child that is used to locate
     * it in 'this' node.
     */
    public Key getChildKey(IN child)
        throws DatabaseException {

        return child.getIdentifierKey();
    }

    /*
     * An IN uses the main key in its searches.
     */
    public Key selectKey(Key mainTreeKey, Key dupTreeKey) {
        return mainTreeKey;
    }

    /**
     * Return the key for this duplicate set.
     */
    public Key getDupKey()
        throws DatabaseException {

        throw new DatabaseException(shortClassName() + ".getDupKey() called");
    }

    /**
     * Return the key for navigating through the duplicate tree.
     */
    Key getDupTreeKey() {
        return null;
    }
    /**
     * Return the key for navigating through the main tree.
     */
    public Key getMainTreeKey() {
        return getIdentifierKey();
    }

    /**
     * Get the database for this IN.
     */
    public DatabaseImpl getDatabase() {
        return databaseImpl;
    }

    /**
     * Set the database reference for this node.
     */
    public void setDatabase(DatabaseImpl db) {
        databaseImpl = db;
    }
        
    /* 
     * Get the database id for this node.
     */
    public DatabaseId getDatabaseId() {
        return databaseImpl.getId();
    }
    /**
     * This is always false for non-BIN INs. If the entry is present, there
     * is a valid child.
     * @param index indicates target entry
     * @return false if the entry is valid and not deleted.
     */
    public boolean isKnownDeleted(int index) {
        return false;
    }

    /**
     * @return the number of entries in this node.
     */
    public int getNEntries() {
        return nEntries;
    }

    /**
     * @return the maximum number of entries in this node.
     */
    int getMaxEntries() {
        return entries.length;
    }

    /**
     * @return the n'th entry of this node.
     */
    public ChildReference getEntry(int n) {
        return entries[n];
    }

    /**
     * @return the target of the n'th entry.
     */
    Node fetchTarget(int n) 
        throws DatabaseException {

        return getEntry(n).fetchTarget(databaseImpl, this);
    }

    /*
     * All methods that modify the entry array must adjust memory sizeing.
     */

    /**
     * Set the n'th entry of this node.
     */
    public void setEntry(int n, ChildReference ref) {
        
        updateMemorySize(entries[n], ref);
        entries[n] = ref;
        if (++n > nEntries) {
            nEntries = n;
        }
        setDirty(true);
    }

    /**
     * Update the n'th entry of this node.
     *
     * Note: does not dirty the node.
     */
    public void updateEntry(int n, Node node) {
        ChildReference ref = entries[n];
        long oldSize = ref.getInMemorySize();
        ref.setTarget(node);
        long newSize = ref.getInMemorySize();
        updateMemorySize(oldSize, newSize);
    }

    /**
     * Update the n'th entry of this node.
     */
    public void updateEntry(int n, Node node, DbLsn lsn) {
        ChildReference ref = entries[n];
        long oldSize = ref.getInMemorySize();
        ref.setLsn(lsn);
        ref.setTarget(node);
        long newSize = ref.getInMemorySize();
        updateMemorySize(oldSize, newSize);
        setDirty(true);
    }

    /**
     * Update the n'th entry of this node.
     */
    public void updateEntry(int n, Node node, DbLsn lsn, Key key) {
        ChildReference ref = entries[n];
        long oldSize = ref.getInMemorySize();
        ref.setLsn(lsn);
        ref.setTarget(node);
        ref.setKey(key);
        long newSize = ref.getInMemorySize();
        updateMemorySize(oldSize, newSize);
        setDirty(true);
    }

    /**
     * Update the n'th entry of this node.
     */
    public void updateEntry(int n, DbLsn lsn) {
        ChildReference ref = entries[n];
        updateMemorySize(ref.getLsn(), lsn);
        ref.setLsn(lsn);
        setDirty(true);
    }

    /**
     * Update the n'th entry of this node.  Only update the key if the
     * new key is less than the existing key.
     */
    private void updateEntryCompareKey(int n,
					 Node node,
					 DbLsn lsn,
					 Key key) {
        ChildReference ref = entries[n];
        long oldSize = ref.getInMemorySize();
        ref.setLsn(lsn);
        ref.setTarget(node);
	Key existingKey = ref.getKey();
        Comparator userCompareToFcn = getKeyComparator();
	int s =
	    (userCompareToFcn == null) ?
	    key.compareTo(existingKey) :
	    userCompareToFcn.compare(key.getKey(), existingKey.getKey());
	if (s < 0) {
	    ref.setKey(key);
	}
        long newSize = ref.getInMemorySize();
        updateMemorySize(oldSize, newSize);
        setDirty(true);
    }

    /*
     * Memory usage calculations.
     */
    public boolean verifyMemorySize() {

        long calcMemorySize = computeMemorySize();
        if (calcMemorySize != inMemorySize) {

            String msg = "-Warning: Out of sync. " +
                "Should be " + calcMemorySize +
                " / actual: " +
                inMemorySize + " node: " + getNodeId();
            Tracer.trace(Level.INFO,
                         databaseImpl.getDbEnvironment(),
                         msg);
                         
            System.out.println(msg);

            return false;
        } else {
            return true;
        }
    }

    /**
     * Return the number of bytes used by this IN.  Latching is up to
     * the caller.
     */
    public long getInMemorySize() {
        return inMemorySize;
    }

    /**
     * Count up the memory usage attributable to this node alone. LNs children
     * are counted by their BIN/DIN parents, but INs are not counted by 
     * their parents because they are resident on the IN list.
     */
    protected long computeMemorySize() {
        MemoryBudget mb = databaseImpl.getDbEnvironment().getMemoryBudget();
        long calcMemorySize = getMemoryOverhead(mb);
        for (int i = 0; i < nEntries; i++) {
            calcMemorySize += getEntry(i).getInMemorySize();
        }
        return calcMemorySize;
    }

    /* Called once at environment startup by MemoryBudget */
    public static long computeOverhead(DbConfigManager configManager) 
        throws DatabaseException {

        int capacity = configManager.getInt(EnvironmentParams.NODE_MAX);

        /* 
         * Overhead consists of all the fields in this class plus the
         * entries array.
         */
        return MemoryBudget.IN_FIXED_OVERHEAD +
            (MemoryBudget.ARRAY_ITEM_OVERHEAD * capacity);
    }
    
    protected long getMemoryOverhead(MemoryBudget mb) {
        return mb.getINOverhead();
    }

    protected void updateMemorySize(ChildReference oldRef,
                                  ChildReference newRef) {
        long delta = 0;
        if (newRef != null) {
            delta = newRef.getInMemorySize();
        }

        if (oldRef != null) {
            delta -= oldRef.getInMemorySize();
        }
        changeMemorySize(delta);
    }

    public void updateMemorySize(long oldSize, long newSize) {
        long delta = newSize - oldSize;
        changeMemorySize(delta);
    }

    void updateMemorySize(Node oldNode, Node newNode) {
        long delta = 0;
        if (newNode != null) {
            delta = newNode.getMemorySizeIncludedByParent();
        }

        if (oldNode != null) {
            delta -= oldNode.getMemorySizeIncludedByParent();
        }
        changeMemorySize(delta);
    }

    protected void updateMemorySize(DbLsn oldLsn, DbLsn newLsn) {
        long oldSize = (oldLsn == null) ? 0 : MemoryBudget.LSN_SIZE;
        long newSize = (newLsn == null) ? 0 : MemoryBudget.LSN_SIZE;
        long delta = newSize - oldSize;
        changeMemorySize(delta);
    }

    private void changeMemorySize(long delta) {
        inMemorySize += delta;

        /*
         * Only update the environment cache usage stats if this IN
         * is actually on the IN list. For example, when we create new
         * INs, they are manipulated off the IN list before
         * being added; if we updated the environment wide cache then,
         * we'd end up double counting.
         */
        if (inListResident) {
            MemoryBudget mb =
                databaseImpl.getDbEnvironment().getMemoryBudget();
            mb.updateCacheMemoryUsage(delta);
        }
    }

    public void setInListResident(boolean resident) {
        inListResident = resident;
    }

    /**
     * Find the entry in this IN for which key arg is >= the key.
     *
     * Currently uses a binary search, but eventually, this may use
     * binary or linear search depending on key size, number of
     * entries, etc.
     *
     * Note that the 0'th entry's key is treated specially in an IN.  It
     * always compares lower than any other key.
     *
     * This is public so that DbCursorTest can access it.
     *
     * @param key - the key to search for.
     * @param indicateIfDuplicate - true if EXACT_MATCH should
     * be or'd onto the return value if key is already present in this node.
     * @param exact - true if an exact match must be found.
     * @return offset for the entry that has a key >= the arg.  0 if key
     * is less than the 1st entry.  -1 if exact is true and no exact match
     * is found.  If indicateIfDuplicate is true and an exact match was found
     * then EXACT_MATCH is or'd onto the return value.
     */
    public int findEntry(Key key,
                         boolean indicateIfDuplicate,
                         boolean exact) {
        int high = nEntries - 1;
        int low = 0;
        int middle = 0;

        Comparator userCompareToFcn = getKeyComparator();

        /*
         * IN's are special in that they have a entry[0] where the key
         * is a virtual key in that it always compares lower than any
         * other key.  BIN's don't treat key[0] specially.  But if the
         * caller asked for an exact match or to indicate duplicates,
         * then use the key[0] and forget about the special entry zero
         * comparison.
         */
        boolean entryZeroSpecialCompare =
            entryZeroKeyComparesLow() && !exact && !indicateIfDuplicate;

        assert nEntries >= 0;
        
        while (low <= high) {
            middle = (high + low) / 2;
            int s;
            Key middleKey = null;
            if (middle == 0 && entryZeroSpecialCompare) {
                s = 1;
            } else {
                middleKey = entries[middle].getKey();
                s = (userCompareToFcn == null) ?
                    key.compareTo(middleKey) :
                    userCompareToFcn.compare(key.getKey(), middleKey.getKey());
            }
            if (s < 0) {
                high = middle - 1;
            } else if (s > 0) {
                low = middle + 1;
            } else {
                int ret;
                if (indicateIfDuplicate) {
                    ret = middle | EXACT_MATCH;
                } else {
                    ret = middle;
                }

                if ((ret >= 0) && exact && isKnownDeleted(ret & 0xffff)) {
                    return -1;
                } else {
                    return ret;
                }
            }
        }

        /* 
	 * No match found.  Either return -1 if caller wanted exact matches
         * only, or return entry for which arg key is > entry key.
	 */
        if (exact) {
            return -1;
        } else {
            return high;
        }
    }

    /**
     * Inserts the argument ChildReference into this IN.  Assumes this
     * node is already latched by the caller.
     *
     * @param entry The ChildReference to insert into the IN.
     *
     * @return true if the entry was successfully inserted, false
     * if it was a duplicate.
     *
     * @throws InconsistentNodeException if the node is full
     * (it should have been split earlier).
     */
    public boolean insertEntry(ChildReference entry)
        throws DatabaseException {

        return (insertEntry1(entry) & INSERT_SUCCESS) != 0;
    }

    /**
     * Same as insertEntry except that it returns the index where the
     * dup was found instead of false.  The return value is |'d with
     * either INSERT_SUCCESS or EXACT_MATCH depending on whether the
     * entry was inserted or it was a duplicate, resp.
     *
     * This returns a failure if there's a duplicate match. The caller must
     * do the processing to check if the entry is actually deleted and
     * can be overwritten. This is foisted upon the caller rather than handled
     * in this object because there may be some latch releasing/retaking 
     * in order to check a child ln.
     *
     * Inserts the argument ChildReference into this IN.  Assumes this
     * node is already latched by the caller.
     *
     * @param entry The ChildReference to insert into the IN.
     *
     * @return either (1) the index of location in the IN where
     * the entry was inserted |'d with INSERT_SUCCESS, or (2) the
     * index of the duplicate in the IN |'d with EXACT_MATCH if the
     * entry was found to be a duplicate.
     *
     * @throws InconsistentNodeException if the node is full
     * (it should have been split earlier).
     */
    public int insertEntry1(ChildReference entry)
        throws DatabaseException {

	if (nEntries >= entries.length) {
	    compress(null);
	}
        if (nEntries < entries.length) {
            Key key = entry.getKey();

            /* 
             * Search without requiring an exact match, but do let us know
             * the index of the match if there is one.
             */
            int index = findEntry(key, true, false);

            if (index >= 0 && (index & EXACT_MATCH) != 0) {
                /* 
                 * There is an exact match.  Don't insert; let the caller
                 * decide what to do with this duplicate.
                 */
                return index;
            } else {
                /* 
                 * There was no key match, so insert to the right of this
                 * entry.
                 */
                index++;
            }

            /* We found a spot for insert, shift entries as needed. */
            if (index < nEntries) {
                shiftEntriesRight(index);
            }
            entries[index] = entry;
            nEntries++;
            adjustCursorsForInsert(index);
            updateMemorySize(0, entry.getInMemorySize());
            setDirty(true);
            return (index | INSERT_SUCCESS);
        } else {
            throw new InconsistentNodeException
                ("Node " + getNodeId() +
                 " should have been split before calling insertEntry");
        }
    }

    /**
     * Deletes the ChildReference with the key arg from this IN.
     * Assumes this node is already latched by the caller.
     *
     * This seems to only be used by INTest.
     *
     * @param key The key of the reference to delete from the IN.
     *
     * @param maybeValidate true if assert validation should occur prior
     * to delete.  Set this to false during recovery.
     *
     * @return true if the entry was successfully deleted, false
     * if it was not found.
     */
    boolean deleteEntry(Key key, boolean maybeValidate)
        throws DatabaseException {

        if (nEntries == 0) {
            return false; // caller should put this node on the IN cleaner list
        }

        int index = findEntry(key, false, true);
        if (index < 0) {
            return false;
        }

        return deleteEntry(index, maybeValidate);
    }

    /**
     * Deletes the ChildReference at index from this IN.
     * Assumes this node is already latched by the caller.
     *
     * @param index The index of the entry to delete from the IN.
     *
     * @param maybeValidate true if asserts are enabled.
     *
     * @return true if the entry was successfully deleted, false
     * if it was not found.
     */
    public boolean deleteEntry(int index, boolean maybeValidate)
        throws DatabaseException {

        if (nEntries == 0) {
            return false;
        }

        /* Check the subtree validation only if maybeValidate is true */
        assert maybeValidate ? 
            validateSubtreeBeforeDelete(index) :
            true;

        if (index < nEntries) {
            updateMemorySize(entries[index].getInMemorySize(), 0);
            for (int i = index; i < nEntries - 1; i++) {
                entries[i] = entries[i + 1];
            }
            nEntries--;
            setDirty(true);
            setCompressedSinceLastLog();

            /* 
	     * Note that we don't have to adjust cursors for delete, should
             * be nothing pointing at this record.
             */
            traceDelete(Level.FINEST, index);
            return true;
        } else {
            return false;
        }
    }

    /**
     * Do nothing since INs don't support deltas.
     */
    protected void setCompressedSinceLastLog() {
    }

    /**
     * Do nothing since INs don't support deltas.
     */
    public void setCleanedSinceLastLog() {
    }

    public boolean compress(BINReference binRef)
        throws DatabaseException {

	return false;
    }

    /* 
     * Validate the subtree that we're about to delete.  Make sure
     * there aren't more than one valid entry on each IN and that the last
     * level of the tree is empty. Also check that there are no cursors
     * on any bins in this subtree. Assumes caller is holding the latch
     * on this parent node.
     *
     * While we could latch couple down the tree, rather than hold latches
     * as we descend, we are presumably about to delete this subtree so
     * concurrency shouldn't be an issue.
     *
     * @return true if the subtree rooted at the entry specified by "index"
     * is ok to delete.
     */
    boolean validateSubtreeBeforeDelete(int index)
        throws DatabaseException {
        
	boolean needToLatch = !getLatch().isOwner();
	try {
	    if (needToLatch) {
		latch();
	    }
	    if (index >= nEntries) {

		/* 
		 * There's no entry here, so of course this entry is deletable.
		 */
		return true;
	    } else {
		Node child = getEntry(index).fetchTarget(databaseImpl, this);
		return child.isValidForDelete();
	    }
	} finally {
	    if (needToLatch &&
		getLatch().isOwner()) {
		releaseLatch();
	    }
	}
    }

    /**
     * Makes a prefix key suitable for this IN by taking the argument
     * key, comparing it to other existing keys in the node and
     * finding the minimal prefix of the argument that will
     * distinguish it in this IN.
     *
     * @param key The key to turn into a prefix key.
     *
     * @return The prefix key
     */
    Key makePrefixKey(Key wholeKey) {
        return wholeKey;
    }

    /**
     * Return true if this node needs splitting.  For the moment, needing
     * to be split is defined by there being no free entries available.
     */
    public boolean needsSplitting() {
        if ((entries.length - nEntries) < 1) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Indicates whether whether entry 0's key is "special" in that it
     * always compares less than any other key.  BIN's don't have the
     * special key, but IN's do.
     */
    boolean entryZeroKeyComparesLow() {
        return true;
    }

    /**
     * Split this into two nodes.  Parent IN is passed in parent and should
     * be latched by the caller.
     *
     * childIndex is the index in parent of where "this" can be found.
     */
    DbLsn split(IN parent, int childIndex, int maxEntries)
        throws DatabaseException {

	return splitInternal(parent, childIndex, maxEntries, -1);
    }

    protected DbLsn splitInternal(IN parent,
                                  int childIndex,
                                  int maxEntries,
                                  int splitIndex)
        throws DatabaseException {

        /* 
         * Find the index of the existing identifierKey so we know which
         * IN (new or old) to put it in.
         */
        if (identifierKey == null) {
            throw new InconsistentNodeException("idkey is null");
        }
        int idKeyIndex = findEntry(identifierKey, false, false);

	if (splitIndex < 0) {
	    splitIndex = nEntries / 2;
	}

        int low, high;
        IN newSibling = null;

        if (idKeyIndex < splitIndex) {

            /* 
             * Current node (this) keeps left half entries.  Right
             * half entries will go in the new node.
             */
            low = splitIndex;
            high = nEntries;
        } else {

            /* 
	     * Current node (this) keeps right half entries.  Left
             * half entries and entry[0] will go in the new node.
	     */
            low = 0;
            high = splitIndex;
        }

        Key newIdKey = entries[low].getKey();
	DbLsn parentLsn = null;

        newSibling = createNewInstance(newIdKey, maxEntries, level);
        newSibling.latch();
        long oldMemorySize = inMemorySize;
	try {
        
	    int toIdx = 0;
	    for (int i = low; i < high; i++) {
		newSibling.setEntry(toIdx++, entries[i]);
	    }

	    int newSiblingNEntries = (high - low);

	    /* 
	     * Remove the entries that we just copied into newSibling
	     * from this node.
	     */
	    if (low == 0) {
		shiftEntriesLeft(newSiblingNEntries);
	    }

	    newSibling.nEntries = toIdx;
	    nEntries -= newSiblingNEntries;
	    setDirty(true);

	    adjustCursors(newSibling, low, high);

	    /* 
	     * Parent refers to child through an element of the
	     * entries array.  Depending on which half of the BIN we
	     * copied keys from, we either have to adjust one pointer
	     * and add a new one, or we have to just add a new pointer
	     * to the new sibling.
	     *
	     * Note that we must use the provisional form of logging
	     * because all three log entries must be read
	     * atomically. The parent must get logged last, as all
	     * referred-to children must preceed it. Provisional
	     * entries guarantee that all three are processed as a
	     * unit. Recovery skips provisional entries, so the
	     * changed children are only used if the parent makes it
	     * out to the log.
	     */
	    EnvironmentImpl env = databaseImpl.getDbEnvironment();
	    LogManager logManager = env.getLogManager();
	    INList inMemoryINs = env.getInMemoryINs();

	    DbLsn newSiblingLsn = newSibling.logProvisional(logManager);
	    DbLsn myNewLsn = logProvisional(logManager);

	    /*
	     * When we update the parent entry, we use
	     * updateEntryCompareKey so that we don't replace the
	     * parent's key that points at 'this' with a key that is >
	     * than the existing one.  Replacing the parent's key with
	     * something > would effectively render a piece of the
	     * subtree inaccessible.  So only replace the parent key
	     * with something <= the existing one.  See
	     * tree/SplitTest.java for more details on the scenario.
	     */
	    if (low == 0) {

		/* 
		 * Change the original entry to point to the new child
		 * and add an entry to point to the newly logged
		 * version of this existing child.
		 */
                if (childIndex == 0) {
                    parent.updateEntryCompareKey(childIndex, newSibling,
                                                 newSiblingLsn, newIdKey);
                } else {
                    parent.updateEntry(childIndex, newSibling, newSiblingLsn);
                }

		boolean insertOk = parent.insertEntry
		    (new ChildReference(this, entries[0].getKey(), myNewLsn));
		assert insertOk;
	    } else {

		/* 
		 * Update the existing child's LSN to reflect the
		 * newly logged version and insert new child into
		 * parent.
		 */
		if (childIndex == 0) {

		    /*
		     * This's idkey may be < the parent's entry 0 so
		     * we need to update parent's entry 0 with the key
		     * for 'this'.
		     */
		    parent.updateEntryCompareKey
			(childIndex, this, myNewLsn, entries[0].getKey());
		} else {
		    parent.updateEntry(childIndex, this, myNewLsn);
		}
		boolean insertOk = parent.insertEntry
		    (new ChildReference(newSibling, newIdKey, newSiblingLsn));
		assert insertOk;
	    }

	    parentLsn = parent.log(logManager);

            /* 
             * Update size. newSibling and parent are correct, but
             * this IN has had its entries shifted and is not correct.
             */
            long newSize = computeMemorySize();
            updateMemorySize(oldMemorySize, newSize);
	    inMemoryINs.add(newSibling);

	    /* 
	     * This parent must be marked dirty to propagate dirtyness
	     * up the tree.
	     */
	    parent.setDirty(true);
        
	    /* Debug log this information. */
	    traceSplit(Level.FINE, parent,
		       newSibling, parentLsn, myNewLsn,
		       newSiblingLsn, splitIndex, idKeyIndex, childIndex);
	} finally {
	    newSibling.releaseLatch();
	}

        return parentLsn;
    }

    /**
     * Called when we know we are about to split on behalf of a key
     * that is the minimum (leftSide) or maximum (!leftSide) of this
     * node.  This is achieved by just forcing the split to occur
     * either one element in from the left or the right
     * (i.e. splitIndex is 1 or nEntries - 1).
     */
    void splitSpecial(IN parent, int parentIndex, int maxEntriesPerNode,
		      Key key, boolean leftSide)
	throws DatabaseException {

	int index = findEntry(key, false, false);
	if (leftSide &&
	    index == 0) {
	    splitInternal(parent, parentIndex, maxEntriesPerNode, 1);
	} else if (!leftSide &&
		   index == (nEntries - 1)) {
	    splitInternal(parent, parentIndex, maxEntriesPerNode,
			  nEntries - 1);
	} else {
	    split(parent, parentIndex, maxEntriesPerNode);
	}
    }

    void adjustCursors(IN newSibling,
                       int newSiblingLow,
                       int newSiblingHigh) {
        /* Cursors never refer to IN's. */
    }

    void adjustCursorsForInsert(int insertIndex) {
        /* Cursors never refer to IN's. */
    }

    /**
     * Return the relevant user defined comparison function for this type
     * of node.  For IN's and BIN's, this is the BTree Comparison function.
     */
    public Comparator getKeyComparator() {
        return databaseImpl.getBtreeComparator();
    }

    /**
     * Shift entries to the right starting with (and including) the
     * entry at index. Caller is responsible for incrementing nEntries.
     * @param index - The position to start shifting from.
     */
    private void shiftEntriesRight(int index) {
        for (int i = nEntries; i > index; i--) {
            entries[i] = entries[i - 1];
        }
        setDirty(true);
    }

    /**
     * Shift entries starting at the byHowMuch'th element to the left, thus
     * removing the first byHowMuch'th elements of the entries array.  This
     * always starts at the 0th entry.
     * Caller is responsible for decrementing nEntries.
     * @param byHowMuch - The number of entries to remove from the left side
     * of the entries array.
     */
    private void shiftEntriesLeft(int byHowMuch) {
        for (int i = 0; i < nEntries - byHowMuch; i++) {
            entries[i] = entries[i + byHowMuch];
        }
        setDirty(true);
    }

    /**
     * Check that the IN is in a valid state.  For now, validity means that
     * the keys are in sorted order and that there are more than 0 entries.
     * maxKey, if non-null specifies that all keys in this node must be
     * less than maxKey.
     */
    public void verify(Key maxKey)
        throws DatabaseException {

	boolean unlatchThis = false;
	try {
	    if (!getLatch().isOwner()) {
		latch();
		unlatchThis = true;
	    }
	    Comparator userCompareToFcn =
		(databaseImpl == null ? null : getKeyComparator());

	    Key key1 = null;
	    for (int i = 1; i < nEntries; i++) {
		key1 = entries[i].getKey();
		Key key2 = entries[i-1].getKey();

		int s = (userCompareToFcn == null) ?
		    key1.compareTo(key2) :
		    userCompareToFcn.compare(key1.getKey(), key2.getKey());
		if (s <= 0) {
		    throw new InconsistentNodeException
			("IN " + getNodeId() + " key " + (i-1) +
			 " (" + key2.toString() +
			 ") and " +
			 i + " (" + key1.toString() +
			 ") are out of order");
		}
	    }

	    boolean inconsistent = false;
	    if (maxKey != null && key1 != null) {
		if (userCompareToFcn == null) {
		    if (key1.compareTo(maxKey) >= 0) {
			inconsistent = true;
		    }
		} else {
		    if (userCompareToFcn.compare
			(key1.getKey(), maxKey.getKey()) >= 0) {
			inconsistent = true;
		    }
		}
	    }

	    if (inconsistent) {
		throw new InconsistentNodeException
		    ("IN " + getNodeId() +
		     " has entry larger than next entry in parent.");
	    }
	} catch (DatabaseException DE) {
	    DE.printStackTrace(System.out);
	} finally {
	    if (unlatchThis) {
		releaseLatch();
	    }
	}
    }

    /**
     * Add self and children to this in-memory IN list. Called by recovery,
     * can run with no latching.
     */
    void rebuildINList(INList inList) 
        throws DatabaseException {

        /* 
         * Recompute your in memory size first and then add yourself to
         * the list.
         */
        initMemorySize();
        inList.add(this);

        /* 
         * Add your children if they're resident. (LNs know how to
         * stop the flow).
         */
        for (int i = 0; i < nEntries; i++) {
            Node n = getEntry(i).getTarget();
            if (n != null) {
                n.rebuildINList(inList);
            }
        }
    }

    /**
     * Remove self and children from the in-memory IN list. The INList latch
     * is already held before this is called.
     */
    void removeFromINList(INList inList) 
        throws DatabaseException {

        if (nEntries > 1) {
            throw new DatabaseException
                ("Found non-deletable IN " + getNodeId() +
                 " while flushing INList. nEntries = " + nEntries);
        }

        /* Remove self. */
        inList.removeLatchAlreadyHeld(this);

        /* Remove your children if they're resident. (LNs know how to stop.) */
        for (int i = 0; i < nEntries; i++) {
            Node n =
		getEntry(i).fetchTargetIgnoreKnownDeleted(databaseImpl, this);
            if (n != null) {
                n.removeFromINList(inList);
            }
        }
    }

    /**
     * Check if this node fits the qualifications for being part of a deletable
     * subtree. It can only have one IN child and no LN children.
     */
    boolean isValidForDelete()
        throws DatabaseException {

	boolean needToLatch = !getLatch().isOwner();
	try {
	    if (needToLatch) {
		latch();
	    }

	    /* 
	     * Can only have one valid child, and that child should be
	     * deletable.
	     */
	    if (nEntries > 1) {            // more than 1 entry.
		return false;
	    } else if (nEntries == 1) {    // 1 entry, check child
		Node child = getEntry(0).fetchTarget(databaseImpl, this);
		return child.isValidForDelete();
	    } else {                       // 0 entries.
		return true;
	    }
	} finally {
	    if (needToLatch &&
		getLatch().isOwner()) {
		releaseLatch();
	    }
	}
    }

    /**
     * See if you are the parent of this child. If not, find a child
     * of your's that may be the parent, and return it. If there are
     * no possiblities, return null. Note that the keys of the target
     * are passed in so we don't have to latch the target to look at
     * them. Also, this node is latched upon entry.
     */
    void findParent(Tree.SearchType searchType,
                    boolean requireExactMatch,
                    SearchResult result,
                    IN target,
                    Key mainTreeKey,
                    Key dupTreeKey,
                    List trackingList)
        throws DatabaseException {

        assert getLatch().isOwner();

        /* We are this node -- there's no parent in this subtree. */
        if (getNodeId() == target.getNodeId()) {
            releaseLatch();
            result.exactParentFound = false;  // no parent exists
            result.keepSearching = false;
            result.parent = null;
            return;
        }

        /* Find an entry */
        if (getNEntries() == 0) {

            /*
             * No more children, can't descend anymore. Return this node, you
             * could be the parent.
             */
            result.keepSearching = false;
            result.exactParentFound = false;
            if (requireExactMatch) {
                releaseLatch();
                result.parent = null;
            } else {
                result.parent = this;
                result.index = -1;
            }
            return;
        } else {
            if (searchType == Tree.SearchType.NORMAL) {
                /* Look for the entry matching key in the current node. */
                result.index = findEntry(selectKey(mainTreeKey, dupTreeKey),
                                         false, false);
            } else if (searchType == Tree.SearchType.LEFT) {
                /* Left search, always take the 0th entry. */
                result.index = 0;
            } else if (searchType == Tree.SearchType.RIGHT) {
                /* Right search, always take the highest entry. */
                result.index = nEntries - 1;
            } else {
                throw new IllegalArgumentException
                    ("Invalid value of searchType: " + searchType);
            }

            if (result.index < 0) {
                result.keepSearching = false;
                result.exactParentFound = false;
                if (requireExactMatch) {
                    releaseLatch();
                    result.parent = null;
                } else {
                    /* this node is going to be the prospective parent. */
                    result.parent = this;
                }
                return;
            }
            
            /* Get the child node that matches. */
            ChildReference childRef = getEntry(result.index);
            if (childRef.isKnownDeleted()) {
                result.exactParentFound = false;
                result.keepSearching = false;
                if (requireExactMatch) {
                    result.parent = null;
                    releaseLatch();
                } else {
                    result.parent = this;
                }
                return;
            }
            Node child = childRef.fetchTarget(databaseImpl, this);
            DbLsn childLsn = childRef.getLsn();

            /* 
             * Note that if the child node needs latching, it's done
             * in isSoughtNode.
             */
            if (child.isSoughtNode(target.getNodeId())) {
                /* We found the child, so this is the parent. */
                result.exactParentFound = true;
                result.parent = this;
                result.keepSearching = false;
                return;
            } else {

                /* 
                 * Decide whether we can descend, or the search is going to
                 * be unsuccessful or whether this node is going to be the
                 * future parent. It depends on what this node is, the
                 * target, and the child.
                 */
                descendOnParentSearch(result, target, child,
                                      requireExactMatch);

                /* If we're tracking, save the lsn and node id */
                if (trackingList != null) {
                    if ((result.parent != this) && (result.parent != null)) {
                        trackingList.add(new TrackingInfo(childLsn, 
                                                          child.getNodeId()));
                    }
                }
                return; 
            }
        }
    }

    /*
     * If this search can go further, return the child. If it can't, and you
     * are a possible new parent to this child, return this IN. If the 
     * search can't go further and this IN can't be a parent to this child,
     * return null.
     */
    protected void descendOnParentSearch(SearchResult result,
                                         IN target,
                                         Node child,
                                         boolean requireExactMatch) 
        throws DatabaseException {

        if (child.canBeAncestor(target)) {
            /* We can search further. */
            releaseLatch();     
            result.parent = (IN) child;
        } else {

            /*
             * Our search ends, we didn't find it. If we need an exact match,
             * give up, if we only need a potential match, keep this node
             * latched and return it.
             */
            ((IN) child).releaseLatch();
            result.exactParentFound = false;
            result.keepSearching = false;

            if (requireExactMatch) {
                releaseLatch();
                result.parent = null;
            } else {
                result.parent = this;
            }
        }
    }

    /*
     * @return true if this IN is the child of the search chain. Note that
     * if this returns false, the child remains latched.
     */
    protected boolean isSoughtNode(long nid)
        throws DatabaseException {

        latch();
        if (getNodeId() == nid) {
            releaseLatch();
            return true;
        } else {
            return false;
        }
    }

    /* 
     * An IN can be an ancestor of any internal node.
     */
    protected boolean canBeAncestor(IN target) {
        return true;
    }

    /*
     * An IN is evictable if none of its children are resident and it is not
     * in use by the cleaner.
     */
    public boolean isEvictable() {

        if (evictionProhibited) {
            return false;
        }
        for (int i = 0; i < nEntries; i++) {
            ChildReference ref = getEntry(i);

            if (ref.getTarget() != null) {
                /* A resident child exists -- this is not evictable. */
                return false;
            }
        }
        return true;
    }

    public void setEvictionProhibited(boolean pinned) {
        evictionProhibited = pinned;
    }

    /*
     * DbStat support.
     */
    void accumulateStats(TreeWalkerStatsAccumulator acc) {
	acc.processIN(this, new Long(getNodeId()), getLevel());
    }

    /*
     * Logging support
     */

    /**
     * Log this IN and clear the dirty flag.
     */
    public DbLsn log(LogManager logManager)
        throws DatabaseException {

        return logInternal(logManager,
                           false, // do not allow deltas
                           false); // log provisonally
    }

    /**
     * Log this IN and clear the dirty flag.
     */
    public DbLsn log(LogManager logManager, boolean isProvisional)
        throws DatabaseException {

        return logInternal(logManager,
                           false, // do not allow deltas
                           isProvisional); // log provisonally
    }

    /**
     * Log this node provisionally and clear the dirty flag
     * @param item object to be logged
     * @return DbLsn of the new log entry
     */
    public DbLsn logProvisional(LogManager logManager)
        throws DatabaseException {

        return logInternal(logManager,
                           false, // do not allow deltas
                           true); // log provisonally
    }

    /**
     * Log this IN. Called by checkpointer, allow deltas where appropriate.
     * If a delta has been logged, return null.
     */
    public DbLsn logAllowDeltas(LogManager logManager,
                                boolean isProvisional)
        throws DatabaseException {

        return logInternal(logManager,
                           true,   // allow deltas
                           isProvisional); 
    }

    /**
     * Decide how to log this node. INs are always logged in full.
     */
    protected DbLsn logInternal(LogManager logManager, 
                                boolean allowDeltas,
                                boolean isProvisional) 
        throws DatabaseException {

        DbLsn lsn = logManager.log(new INLogEntry(this), isProvisional,
                                   null, false);
        setDirty(false);
        return lsn;
    }

    /**
     * @see LoggableObject#getLogType
     */
    public LogEntryType getLogType() {
        return LogEntryType.LOG_IN;
    }

    /**
     * @see LoggableObject#getLogSize
     */
    public int getLogSize() {
        int size = super.getLogSize();          // ancestors
        size += identifierKey.getLogSize();     // identifier key
        size += LogUtils.getBooleanLogSize(); // isRoot
        size += LogUtils.INT_BYTES;           // nentries;
        size += LogUtils.INT_BYTES;           // length of entries array
        size += LogUtils.INT_BYTES;           // level
        for (int i = 0; i < nEntries; i++) {    // entries
            size += entries[i].getLogSize();
        }
        return size;
    }

    /**
     * @see LoggableObject#writeToLog
     */
    public void writeToLog(ByteBuffer logBuffer) {

        // ancestors
        super.writeToLog(logBuffer); 

        // identifier key
        identifierKey.writeToLog(logBuffer);

        // isRoot
        LogUtils.writeBoolean(logBuffer, isRoot);

        // nEntries
        LogUtils.writeInt(logBuffer, nEntries); 

        // level
        LogUtils.writeInt(logBuffer, level); 

        // length of entries array
        LogUtils.writeInt(logBuffer, entries.length); 

        // entries
        for (int i = 0; i < nEntries; i++) {
            entries[i].writeToLog(logBuffer);
        }
    }

    /**
     * @see LogReadable#readFromLog
     */
    public void readFromLog(ByteBuffer itemBuffer)
        throws LogException {

        // ancestors 
        super.readFromLog(itemBuffer);

        // identifier key
        identifierKey.readFromLog(itemBuffer);

        // isRoot
        isRoot = LogUtils.readBoolean(itemBuffer);

        // nEntries
        nEntries = LogUtils.readInt(itemBuffer);

        // level
        level = LogUtils.readInt(itemBuffer);

        // length
        int length = LogUtils.readInt(itemBuffer);
        entries = new ChildReference[length];

        // entries
        for (int i = 0; i < nEntries; i++) {
            entries[i] = new ChildReference();
            entries[i].readFromLog(itemBuffer);
        }

        latch.setName(shortClassName() + getNodeId());
    }
    
    /**
     * @see LogReadable#dumpLog
     */
    public void dumpLog(StringBuffer sb, boolean verbose) {
        sb.append(beginTag());

        super.dumpLog(sb, verbose);
        identifierKey.dumpLog(sb, verbose);

        // isRoot
        sb.append("<isRoot val=\"");
        sb.append(isRoot);
        sb.append("\"/>");

        // level
        sb.append("<level val=\"");
        sb.append(Integer.toHexString(level));
        sb.append("\"/>");

        // nEntries, length of entries array
        sb.append("<entries numEntries=\"");
        sb.append(nEntries);
        sb.append("\" length=\"");
        sb.append(entries.length);
        sb.append("\">");

        if (verbose) {
            for (int i = 0; i < nEntries; i++) {
                entries[i].dumpLog(sb, verbose);
            }
        }

        sb.append("</entries>");

        // Add on any additional items from subclasses before the end tag
        dumpLogAdditional(sb);

        sb.append(endTag());
    }

    /**
     * @see LogReadable#logEntryIsTransactional.
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
     * Allows subclasses to add additional fields before the end tag. If they
     * just overload dumpLog, the xml isn't nested.
     */
    protected void dumpLogAdditional(StringBuffer sb) {
    }

    public String beginTag() {
        return BEGIN_TAG;
    }

    public String endTag() {
        return END_TAG;
    }

    void dumpKeys() throws DatabaseException {
        for (int i = 0; i < nEntries; i++) {
            ChildReference entry = entries[i];
            System.out.println(entry.getKey().toString());
        }
    }

    /**
     * For unit test support:
     * @return a string that dumps information about this IN, without
     */
    public String dumpString(int nSpaces, boolean dumpTags) {
        StringBuffer sb = new StringBuffer();
        if (dumpTags) {
            sb.append(TreeUtils.indent(nSpaces));
            sb.append(beginTag());
            sb.append('\n');
        }

        sb.append(super.dumpString(nSpaces+2, true));
        sb.append('\n');

        sb.append(TreeUtils.indent(nSpaces+2));
        sb.append("<idkey>");
        sb.append((identifierKey == null ? "" : identifierKey.toString()));
        sb.append("</idkey>");
        sb.append('\n');
        sb.append(TreeUtils.indent(nSpaces+2));
        sb.append("<dirty val=\"").append(dirty).append("\"/>");
        sb.append('\n');
        sb.append(TreeUtils.indent(nSpaces+2));
        sb.append("<generation val=\"").append(generation).append("\"/>");
        sb.append('\n');
        sb.append(TreeUtils.indent(nSpaces+2));
        sb.append("<level val=\"");
        sb.append(Integer.toHexString(level)).append("\"/>");
        sb.append('\n');
        sb.append(TreeUtils.indent(nSpaces+2));
        sb.append("<isRoot val=\"").append(isRoot).append("\"/>");
        sb.append('\n');

        sb.append(TreeUtils.indent(nSpaces+2));
        sb.append("<entries nEntries=\"");
        sb.append(nEntries);
        sb.append("\">");
        sb.append('\n');
        for (int i = 0; i < nEntries; i++) {
            sb.append(TreeUtils.indent(nSpaces+4));
            sb.append("<entry id=\"" + i + "\">");
            sb.append('\n');
            ChildReference cref = entries[i];
            if (cref == null) {
                sb.append(TreeUtils.indent(nSpaces + 6));
                sb.append("<empty>");
            } else {
                sb.append(cref.dumpString(nSpaces+6, true));
            }
            sb.append('\n');
            sb.append(TreeUtils.indent(nSpaces+4));
            sb.append("</entry>");
            sb.append('\n');
        }
        sb.append(TreeUtils.indent(nSpaces+2));
        sb.append("</entries>");
        sb.append('\n');
        if (dumpTags) {
            sb.append(TreeUtils.indent(nSpaces));
            sb.append(endTag());
        }
        return sb.toString();
    }

    public String toString() {
        return dumpString(0, true);
    }

    public String shortClassName() {
        return "IN";
    }

    /**
     * Send trace messages to the java.util.logger. Don't rely on the
     * logger alone to conditionalize whether we send this message,
     * we don't even want to construct the message if the level is
     * not enabled.
     */

    void traceSplit(Level level,
                    IN parent,
                    IN newSibling, DbLsn parentLsn,
                    DbLsn myNewLsn,
                    DbLsn newSiblingLsn,
                    int splitIndex,
                    int idKeyIndex, int childIndex) {
        Logger logger = databaseImpl.getDbEnvironment().getLogger();
        if (logger.isLoggable(level)) {
            StringBuffer sb = new StringBuffer();
            sb.append(TRACE_SPLIT);
            sb.append(" parent=");
            sb.append(parent.getNodeId());
            sb.append(" child=");
            sb.append(getNodeId());
            sb.append(" newSibling=");
            sb.append(newSibling.getNodeId());
            sb.append(" parentLsn = ");
            sb.append(parentLsn.getNoFormatString());
            sb.append(" childLsn = ");
            sb.append(myNewLsn.getNoFormatString());
            sb.append(" newSiblingLsn = ");
            sb.append(newSiblingLsn.getNoFormatString());
            sb.append(" splitIdx=");
            sb.append(splitIndex);
            sb.append(" idKeyIdx=");
            sb.append(idKeyIndex);
            sb.append(" childIdx=");
            sb.append(childIndex);
            logger.log(level, sb.toString());
        }
    }

    /**
     * Send trace messages to the java.util.logger. Don't rely on the
     * logger alone to conditionalize whether we send this message,
     * we don't even want to construct the message if the level is
     * not enabled.
     */
    private void traceDelete(Level level, int index) {
        Logger logger = databaseImpl.getDbEnvironment().getLogger();
        if (logger.isLoggable(level)) {
            StringBuffer sb = new StringBuffer();
            sb.append(TRACE_DELETE);
            sb.append(" in=").append(getNodeId());
            sb.append(" index=");
            sb.append(index);
            logger.log(level, sb.toString());
        }
    }
}
