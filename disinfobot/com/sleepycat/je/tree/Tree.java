/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: Tree.java,v 1.1 2004/11/22 18:27:57 kate Exp $
*/

package com.sleepycat.je.tree;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.ListIterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.CursorImpl;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.INList;
import com.sleepycat.je.latch.Latch;
import com.sleepycat.je.log.LogManager;
import com.sleepycat.je.log.LogReadable;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.log.LogWritable;
import com.sleepycat.je.txn.BasicLocker;
import com.sleepycat.je.txn.LockGrantType;
import com.sleepycat.je.txn.LockResult;
import com.sleepycat.je.txn.Locker;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.Tracer;

/**
 * Tree implements the JE B+Tree.
 * 
 * A note on tree search patterns: 
 * There's a set of Tree.search* methods. Some clients of the tree use
 * those search methods directly, whereas other clients of the tree
 * tend to use methods built on top of search.
 *
 * The semantics of search* are
 *   they leave you pointing at a BIN or IN
 *   they don't tell you where the reference of interest is.
 *   they traverse a single tree, to jump into the duplicate tree, the 
 *   caller has to take explicit action.
 * The semantics of the get* methods are:
 *   they leave you pointing at a BIN or IN
 *   they return the index of the slot of interest
 *   they traverse down to whatever level is needed -- they'll take care of
 *   jumping into the duplicate tree.
 *   they are built on top of search* methods.
 * For the future:
 * Over time, we need to clarify which methods are to be used by clients
 * of the tree. Preferably clients that call the tree use get*, although 
 * their are cases where they need visibility into the tree structure. For 
 * example, tee cursors use search* because they want to add themselves to 
 * BIN before jumping into the duplicate tree.
 * 
 * Also, search* should return the location of the slot to save us a 
 * second binary search.
 */
public final class Tree implements LogWritable, LogReadable {

    /* For debug tracing */
    private static final String TRACE_ROOT_SPLIT = "RootSplit:";
    private static final String TRACE_DUP_ROOT_SPLIT = "DupRootSplit:";
    private static final String TRACE_MUTATE = "Mut:";
    private static final String TRACE_INSERT = "Ins:";
    private static final String TRACE_INSERT_DUPLICATE = "InsD:";

    private DatabaseImpl database;
    private ChildReference root;
    private int maxEntriesPerNode;

    /* 
     * Latch that must be held when using/accessing the root node.  Protects
     * against the root being changed out from underneath us by splitRoot.
     */
    private Latch rootLatch;

    private TreeStats treeStats;

    private TreeWalkerStatsAccumulator treeStatsAccumulator;

    /**
     * Embodies an enum for the type of search being performed.  NORMAL means
     * do a regular search down the tree.  LEFT/RIGHT means search down the
     * left/right side to find the first/last node in the tree.  DELETE means
     * to search down the tree using a key, but instead of returning the lowest
     * node matching that key, the lowest node in the path with more than one
     * entry is returned.
     */
    static public class SearchType {
        /* Search types */
        public static final SearchType NORMAL = new SearchType();
        public static final SearchType LEFT   = new SearchType();
        public static final SearchType RIGHT  = new SearchType();
        public static final SearchType DELETE = new SearchType();

        /* No lock types can be defined outside this class. */
        private SearchType() {
        }
    }

    /**
     * Create a new tree.
     */
    public Tree(DatabaseImpl database)
        throws DatabaseException {

        init(database);
        maxEntriesPerNode = getMaxEntriesFromConfig(database);
    }
    
    /**
     * Create a tree that's being read in from the log.
     */
    public Tree() {
        init(null);
        maxEntriesPerNode = 0;
    }

    /**
     * constructor helper
     */
    private void init(DatabaseImpl database) {
        rootLatch =
            new Latch("RootLatch", 
                      (database != null) ? database.getDbEnvironment() : null);
        treeStats = new TreeStats();
	treeStatsAccumulator = null;
        this.root = null;
        this.database = database;
    }

    /**
     * Set the database for this tree. Used by recovery when recreating an
     * existing tree.
     */
    public void setDatabase(DatabaseImpl database)
        throws DatabaseException {

        this.database = database;
        maxEntriesPerNode = getMaxEntriesFromConfig(database);
    }
    
    /**
     * Get the maxEntriesPerNode configuration from a fully created database.
     */
    private int getMaxEntriesFromConfig(DatabaseImpl database)
        throws DatabaseException {

        DbConfigManager configManager = 
            database.getDbEnvironment().getConfigManager();
        return configManager.getInt(EnvironmentParams.NODE_MAX);
    }

    /**
     * @return the database for this Tree.
     */
    public DatabaseImpl getDatabase() {
        return database;
    }

    /**
     * Set the root for the tree. Should only be called within the root latch.
     */
    public void setRoot(ChildReference newRoot) {
        root = newRoot;
    }

    /**
     * @return the TreeStats for this tree.
     */
    TreeStats getTreeStats() {
        return treeStats;
    }

    public void setTreeStatsAccumulator(TreeWalkerStatsAccumulator tSA) {
	treeStatsAccumulator = tSA;
    }

    public IN withRootLatched(WithRootLatched wrl)
        throws DatabaseException {

        try {
            rootLatch.acquire();
            return wrl.doWork(root);
        } finally {
            rootLatch.release();
        }
    }

    /**
     * Deletes a BIN specified by key from the tree.  The reference from the
     * node's parent is removed.  If removing that reference leaves no more
     * entries in that IN, that IN is removed, and so on up the tree.
     *
     * We don't try to delete a BIN if there are cursors referring to it.
     *
     * Upon exiting this routine, the node is removed from the tree and is
     * unlatched, unless search found that the BIN has entries in it (perhaps
     * because someone came along and added entries between the time the BIN
     * was put on the Compressor queue and the time the Compressor got around
     * to compressing.)
     *
     * Assumes the INList major latch is held.
     *
     * @param idKey - the identifier key of the node to delete.
     * @return true if the item deleted was the root of the tree, false
     * otherwise.
     */
    public boolean delete(Key idKey)
        throws DatabaseException {

        INList inMemoryINs = database.getDbEnvironment().getInMemoryINs();
        boolean treeEmpty = false;

        IN subtreeRoot = null;

        /*
         * The following search returns the highest node of a deletable subtree
         * that holds this idKey. Then when IN.deleteEntry is called on this
         * root, IN.deleteEntry will traverse downwards.
         */
        subtreeRoot = search(idKey, SearchType.DELETE, -1);

        LogManager logManager =
            database.getDbEnvironment().getLogManager();
        if (subtreeRoot == null) {

            /*
             * The root is the top of this subtree. If there are no more
             * entries left in the root, delete the whole tree.  There's a
             * window on the rootLatch between the time that search releases
             * the rootLatch and the acquire below.  Something could insert
             * into the tree.  Use validateSubtreeForDelete to ensure that it's
             * still empty.
             */
            rootLatch.acquire();
            try {
                IN rootIN = (IN) root.fetchTarget(database, null);
                
		DbConfigManager configManager = 
		    database.getDbEnvironment().getConfigManager();
		boolean purgeRoot = configManager.getBoolean
		    (EnvironmentParams.COMPRESSOR_PURGE_ROOT);

		/**
		 * We've encountered the last empty subtree of the tree.  In
		 * general, there's no reason to delete this last
		 * IN->...IN->BIN subtree since we're likely to to add more
		 * nodes to this tree again.  Deleting the subtree also adds to
		 * the space used by the log since a MapLN needs to be written
		 * when the root is nulled, and a MapLN, IN (root), BIN needs
		 * to be written when the root is recreated.
		 *
		 * Consider a queue application which frequently inserts and
		 * deletes entries and often times leaves the tree empty, but
		 * will insert new records again.
		 * 
		 * An optimization might be to prune the multiple IN path to
		 * the last BIN (if it even exists) to just a root IN pointing
		 * to the single BIN, but this doesn't feel like it's worth the
		 * trouble since the extra depth doesn't matter all that much.
		 *
		 * If je.compressor.purgeRoot is true, then we null the root.
		 */
                if (purgeRoot &&
		    (rootIN.getNEntries() <= 1) &&
                    (rootIN.validateSubtreeBeforeDelete(0))) {

                    /*
                     * The tree is empty, clear out the IN list.  Can't just
                     * call clear() because there are IN's from more than one
                     * Database on the list.
                     */
                    removeSubtreeFromINList(inMemoryINs, rootIN);
                    root = null;
                    treeEmpty = true;

                    /*
                     * Record the root deletion for recovery. Do this within
                     * the root latch. We need to put this log entry into the
                     * log before another thread comes in and creates a new
                     * rootIN for this database.
                     *
                     * For example,
                     * lsn 1000 IN delete info entry
                     * lsn 1010 new IN, for next set of inserts
                     * lsn 1020 new BIN, for next set of inserts.
                     *
                     * The entry at 1000 is needed so that lsn 1010 will
                     * properly supercede all previous IN entries in the tree.
                     */
                    logManager.log(new INDeleteInfo
                                   (rootIN.getNodeId(),
                                    rootIN.getIdentifierKey(),
                                    database.getId()));
                }
            } finally {
                rootLatch.release();
            }

        } else {
            int index = subtreeRoot.findEntry(idKey, false, false);
            IN subtreeRootIN = (IN)
                subtreeRoot.getEntry(index).fetchTarget(database, subtreeRoot);
            removeSubtreeFromINList(inMemoryINs, subtreeRootIN);
            boolean deleteOk = subtreeRoot.deleteEntry(index, true);
            assert deleteOk;

            /* 
             * Record in the log the nodeid of the highest node in the subtree
             * that we're deleting. We'll use this later to navigate to the
             * right place if we need to replay this delete.
             */
            logManager.log(new INDeleteInfo
                           (subtreeRootIN.getNodeId(),
                            subtreeRootIN.getIdentifierKey(),
                            database.getId()));
            subtreeRoot.getLatch().release();
        }

        return treeEmpty;
    }

    /**
     * Delete a subtree of a duplicate tree.  Find the duplicate tree using
     * dupKey in the top part of the tree and idKey in the duplicate tree.  In
     * the duplicate tree do a SearchType.DELETE to find the largest subtree
     * that can be deleted safely.
     *
     * Assumes the INList major latch is held.
     *
     * @param idKey the identifier key to be used in the duplicate subtree to
     * find the duplicate path.
     *
     * @param dupKey the duplicate key to be used in the main tree to find the
     * duplicate subtree.
     *
     * @return true if the delete succeeded, false if there were still cursors
     * present on the leaf DBIN of the subtree that was located.
     */
    public boolean deleteDup(Key idKey, Key dupKey)
        throws DatabaseException {

        boolean ret = true;

	EnvironmentImpl env = database.getDbEnvironment();
        INList inMemoryINs = env.getInMemoryINs();

        IN in = search(dupKey, SearchType.NORMAL, -1);

        assert in.getLatch().isOwner();
        assert in instanceof BIN;
        assert in.getNEntries() > 0;

	DIN duplicateRoot = null;
	boolean dupCountLNLocked = false;
	DupCountLN dcl = null;
	BasicLocker locker = new BasicLocker(env);

        int index = in.findEntry(dupKey, false, true);
	try {
	    if (index >= 0) {
		ChildReference childRef = in.getEntry(index);
		duplicateRoot = (DIN) childRef.fetchTarget(database, in);
		duplicateRoot.latch();

		ChildReference dclRef = duplicateRoot.getDupCountLNRef();
		dcl = (DupCountLN) dclRef.fetchTarget(database, duplicateRoot);

		/* Read lock the dup count LN. */
		if (locker.nonBlockingReadLock(dcl) == LockGrantType.DENIED) {
		    return false;
		} else {
		    dupCountLNLocked = true;
		}

		/*
		 * We don't release the latch on 'in' before we search the
		 * duplicate tree below because we might be deleting the whole
		 * subtree from the IN and we want to keep it latched until we
		 * know.
		 */
		IN subtreeRoot;
		try {
		    subtreeRoot = searchSubTree(duplicateRoot,
						idKey,
						SearchType.DELETE,
						-1);
		} catch (NodeNotEmptyException NNNE) {

		    /*
		     * We can't delete the subtree because there are still
		     * cursors pointing to the lowest node on it.
		     */
		    in.releaseLatch();
		    throw NNNE;
		}

		if (subtreeRoot == null) {
		    /* We're deleting the duplicate root. */
		    BIN bin = (BIN) in;
		    if (bin.nCursors() == 0) {
			try {

			    /*
			     * duplicateRoot is not currently latched.  Relatch
			     * it and recheck if it still is deletable.
			     */
			    duplicateRoot.latch();
			    if (duplicateRoot.isValidForDelete()) {
				removeSubtreeFromINList(inMemoryINs,
							duplicateRoot);
				boolean deleteOk =
				    bin.deleteEntry(index, true);
				assert deleteOk;

				if (bin.getNEntries() == 0) {
				    database.getDbEnvironment().
					addToCompressorQueue(bin, null);
				}
			    }
			} finally {
			    duplicateRoot.releaseLatch();
			}
		    } else {

			/*
			 * Don't delete anything off this IN if there are
			 * cursors referring to it.
			 */
			ret = false;
		    }
		    in.releaseLatch();
		} else {
		    try {
			/* We're deleting a portion of the duplicate tree. */
			in.releaseLatch();
			int dupIndex = subtreeRoot.findEntry(idKey, false,
							     false);
			IN rootIN = (IN)
			    subtreeRoot.getEntry
			    (dupIndex).fetchTarget(database, subtreeRoot);
			removeSubtreeFromINList(inMemoryINs, rootIN);
			boolean deleteOk =
			    subtreeRoot.deleteEntry(dupIndex, true);
			assert deleteOk;
		    } finally {
			subtreeRoot.getLatch().release();
		    }
		}
	    }
	} finally {
	    /* 
	     * Release this IN, either because it didn't prove to be the target
	     * IN, or because we threw out of the attempt to delete the
	     * subtree.
	     */
	    if (in.getLatch().isOwner()) {
		in.releaseLatch();
	    }
	    if (duplicateRoot.getLatch().isOwner()) {
		duplicateRoot.releaseLatch();
	    }
	    if (dupCountLNLocked) {
		locker.releaseLock(dcl);
	    }
	}

        return ret;
    }

    /**
     * Find the leftmost node (IN or BIN) in the tree.  Do not descend into a
     * duplicate tree if the leftmost entry of the first BIN refers to one.
     *
     * @return the leftmost node in the tree, null if the tree is empty.  The
     * returned node is latched and the caller must release it.
     */
    public IN getFirstNode()
        throws DatabaseException {

        return search(null, SearchType.LEFT, -1);
    }

    /**
     * Find the rightmost node (IN or BIN) in the tree.  Do not descend into a
     * duplicate tree if the rightmost entry of the last BIN refers to one.
     *
     * @return the rightmost node in the tree, null if the tree is empty.  The
     * returned node is latched and the caller must release it.
     */
    public IN getLastNode()
        throws DatabaseException {

        return search(null, SearchType.RIGHT, -1);
    }

    /**
     * Find the leftmost node (DBIN) in a duplicate tree.
     *
     * @return the leftmost node in the tree, null if the tree is empty.  The
     * returned node is latched and the caller must release it.
     */
    public DBIN getFirstNode(DIN duplicateRoot)
        throws DatabaseException {

        if (duplicateRoot == null) {
            throw new IllegalArgumentException
                ("getFirstNode passed null root");
        }

        assert duplicateRoot.getLatch().isOwner();

        IN ret = searchSubTree(duplicateRoot, null, SearchType.LEFT, -1);
        return (DBIN) ret;
    }

    /**
     * Find the rightmost node (DBIN) in a duplicate tree.
     *
     * @return the rightmost node in the tree, null if the tree is empty.  The
     * returned node is latched and the caller must release it.
     */
    public DBIN getLastNode(DIN duplicateRoot)
        throws DatabaseException {

        if (duplicateRoot == null) {
            throw new IllegalArgumentException
                ("getLastNode passed null root");
        }

        assert duplicateRoot.getLatch().isOwner();

        IN ret = searchSubTree(duplicateRoot, null, SearchType.RIGHT, -1);
        return (DBIN) ret;
    }

    /**
     * GetParentNode without optional tracking.
     */
    public SearchResult getParentINForChildIN(IN child,
					      boolean requireExactMatch) 
        throws DatabaseException {

        return getParentINForChildIN(child, requireExactMatch, null);
    }

    /**
     * Return a reference to the parent or possible parent of the child.  Used
     * by objects that need to take a standalone node and find it in the tree,
     * like the evictor, checkpointer, and recovery.
     *
     * @param child The child node for which to find the parent.  This node is
     * latched by the caller and is released by this function before returning
     * to the caller.
     *
     * @param requireExactMatch if true, we must find the exact parent, not a
     * potential parent.
     *
     * @param trackingList if not null, add the lsns of the parents visited
     * along the way, as a debug tracing mechanism. This is meant to stay in
     * production, to add information to the log.
     *
     * @return a SearchResult object. If the parent has been found,
     * result.foundExactMatch is true. If any parent, exact or potential has
     * been found, result.parent refers to that node.
     */
    public SearchResult getParentINForChildIN(IN child,
					      boolean requireExactMatch,
					      List trackingList)
        throws DatabaseException {

        /* Sanity checks */
        if (child == null) {
            throw new IllegalArgumentException("getParentNode passed null");
        }

        assert child.getLatch().isOwner();

        /* Get information from child before releasing latch. */
        Key mainTreeKey = child.getMainTreeKey();
        Key dupTreeKey = child.getDupTreeKey();
        child.releaseLatch();

        IN rootIN = getRootIN();

        SearchResult result = new SearchResult();
        if (rootIN != null) {
            /* The tracking list is a permanent tracing aid. */
            if (trackingList != null) {
                trackingList.add(new TrackingInfo(root.getLsn(),
                                                  rootIN.getNodeId()));
            }
                
            IN potentialParent = rootIN;

            try {
                while (result.keepSearching) {
                    potentialParent.findParent(SearchType.NORMAL,
                                               requireExactMatch,
                                               result,
                                               child,
                                               mainTreeKey,
                                               dupTreeKey,
                                               trackingList);
                    potentialParent = result.parent;
                }
            } catch (Exception e) {
                if (potentialParent.getLatch().isOwner()) {
                    potentialParent.releaseLatch();
                } 
                throw new DatabaseException(e);
            }
        } 
        return result;
    }

    /**
     * Return a reference to the parent of this LN. This searches through the
     * main and duplicate tree and allows splits. Set the tree location to the
     * proper BIN parent whether or not the LN child is found. That's because
     * if the LN is not found, recovery or abort will need to place it within
     * the tree, and so we must point at the appropriate position.
     *
     * @param location a holder class to hold state about the location 
     *                 of our search. Sort of an internal cursor.
     * @param mainKey key to navigate through main key
     * @param dupKey key to navigate through duplicate tree. May be null, since
     * deleted lns have no data.
     * @param ln the node instantiated from the log
     * @param splitsAllowed true if this method is allowed to cause tree splits
     * as a side effect. In practice, recovery can cause splits, but abort
     * can't.
     * @param searchDupTree true if a search through the dup tree looking for
     * a match on the ln's node id should be made (only in the case where
     * dupKey == null).  See SR 8984.
     *
     * @return true if node found in tree.
     * If false is returned and there is the possibility that we can insert 
     * the record into a plausible parent we must also set
     * - location.bin (may be null if no possible parent found)
     * - location.lnKey (don't need to set if no possible parent).
     */
    public boolean getParentBINForChildLN(TreeLocation location,
                                          Key mainKey,
                                          Key dupKey,
                                          LN ln,
                                          boolean splitsAllowed,
					  boolean findDeletedEntries,
					  boolean searchDupTree)
        throws DatabaseException {



        /* 
         * Find the BIN that either points to this LN or could be its
         * ancestor.
         */
        if (splitsAllowed) {
            location.bin = (BIN) searchSplitsAllowed(mainKey, ln.getNodeId());
        } else {
            location.bin = (BIN) search(mainKey,
                                        SearchType.NORMAL,
                                        ln.getNodeId());
        }

	if (location.bin == null) {
	    return false;
	}

	/*
	 * If caller wants us to consider knownDeleted entries then do do an
	 * inexact search in findEntry since that will find knownDeleted
	 * entries.  If caller doesn't want us to consider knownDeleted entries
	 * then do an exact search in findEntry since that will not return
	 * knownDeleted entries.
	 */
	boolean exactSearch = false;
	boolean indicateIfExact = true;
	if (!findDeletedEntries) {
	    exactSearch = true;
	    indicateIfExact = false;
	}
        location.index =
	    location.bin.findEntry(mainKey, indicateIfExact, exactSearch);

	boolean match = false;
	if (findDeletedEntries) {
	    match = (location.index >= 0 &&
		     (location.index & IN.EXACT_MATCH) != 0);
	    location.index &= ~IN.EXACT_MATCH;
	} else {
	    match = (location.index >= 0);
	}

        if (match) {

            /*
             * A BIN parent was found and a slot matches the key. See if
             * we have to search further into what may be a dup tree.
             */
            ChildReference childRef = location.bin.getEntry(location.index);
	    if (!childRef.isKnownDeleted()) {
                
                /*
                 * If this database doesn't support duplicates, no point in
                 * incurring the potentially large cost of fetching in
                 * the child to check for dup trees. In the future, we could
                 * optimize further by storing state per slot as to whether
                 * a dup tree hangs below.
                 */
                if (database.getSortedDuplicates()) {
                    Node childNode = childRef.fetchTarget(database,
                                                          location.bin);

                    /* Is our target LN a regular record or a dup count? */
                    if (ln.containsDuplicates()) {
                        /* This is a duplicate count LN. */
                        return searchDupTreeForDupCountLNParent(location,
                                                                mainKey,
                                                                childNode);
                    } else {

                        /* 
                         * This is a regular LN. If this is a dup tree,
                         * descend and search. If not, we've found the
                         * parent.
                         */
                        if (childNode.containsDuplicates()) {
                            if (dupKey == null) {

                                /*
                                 * We are at a dup tree but our target LN
                                 * has no dup key because it's a deleted LN. 
                                 * We've encountered the case of SR 8984 where
                                 * we are searching for an LN that was deleted
                                 * before the conversion to a duplicate tree.
                                 */
                                return searchDupTreeByNodeId(location,
                                                             childNode,
                                                             ln,
                                                             searchDupTree);
                            } else {
                                return searchDupTreeForDBIN(location,
                                                            dupKey,
                                                            (DIN) childNode,
                                                            ln,
                                                            findDeletedEntries,
                                                            indicateIfExact,
                                                            exactSearch,
                                                            splitsAllowed);
                            }
                        }
                    }
                }
            }

            /* We had a match, we didn't need to search the duplicate tree. */
            location.childLsn = childRef.getLsn();
            return true;
        } else {
            location.lnKey = mainKey;
            return false;
        }
    }

    /**
     * For SR [#8984]: our prospective child is a deleted LN, and
     * we're facing a dup tree. Alas, the deleted LN has no data, and
     * therefore nothing to guide the search in the dup tree. Instead,
     * we search by node id.  This is very expensive, but this
     * situation is a very rare case.
     */
    private boolean searchDupTreeByNodeId(TreeLocation location,
                                          Node childNode,
                                          LN ln,
                                          boolean searchDupTree) 
        throws DatabaseException {

        if (searchDupTree) {
            BIN oldBIN = location.bin;
            if (childNode.matchLNByNodeId
                (location, ln.getNodeId())) {
                location.index &= ~IN.EXACT_MATCH;
                if (oldBIN != null) {
                    oldBIN.releaseLatch();
                }
                location.bin.latch();
                return true;
            } else {
                return false;
            }
        } else {
            
            /*
             * This is called from undo() so this LN can
             * just be ignored.
             */
            return false;
        }
    }
    
    /**
     * @return true if childNode is the DIN parent of this DupCountLN
     */
    private boolean searchDupTreeForDupCountLNParent(TreeLocation location,
                                                     Key mainKey,
                                                     Node childNode) 
        throws DatabaseException {
        location.lnKey = mainKey;
        if (childNode instanceof DIN) {
            DIN duplicateRoot = (DIN) childNode;
            location.childLsn = duplicateRoot.getDupCountLNRef().getLsn();
            return true;
        } else {

            /*
             * If we're looking for a DupCountLN but don't find a
             * duplicate tree, then the key now refers to a single
             * datum.  This can happen when all dups for a key are
             * deleted, the compressor runs, and then a single
             * datum is inserted.  [#10597]
             */
            return false;
        }
    }

    /**
     * Search the dup tree for the DBIN parent of this ln.
     */
    private boolean searchDupTreeForDBIN(TreeLocation location,
                                         Key dupKey,
                                         DIN duplicateRoot,
                                         LN ln,
                                         boolean findDeletedEntries,
                                         boolean indicateIfExact,
                                         boolean exactSearch,
                                         boolean splitsAllowed)
        throws DatabaseException {                                     
        duplicateRoot.latch();

        /* Make sure there's room for inserts. */
        if (maybeSplitDuplicateRoot(location.bin, location.index)) {
            ChildReference childRef = location.bin.getEntry(location.index);
            duplicateRoot = (DIN)
                childRef.fetchTarget(database, location.bin);
        }

        /* Wait until after any duplicate root splitting to unlatch the bin. */
        location.bin.releaseLatch();                    

        /*
         * The dupKey is going to be the key that represents
         * the LN in this BIN parent.  Get our dupkey from the
         * LN, or from the log if this is a deleted LN.
         */
        location.lnKey = (dupKey == null) ? new Key(ln.getData()) : dupKey;

        /* Search the dup tree */
        if (splitsAllowed) {
            location.bin = (BIN) searchSubTreeSplitsAllowed(duplicateRoot,
                                                           location.lnKey,
                                                           ln.getNodeId());
        } else {
            location.bin = (BIN) searchSubTree(duplicateRoot,
                                               location.lnKey,
                                               SearchType.NORMAL,
                                               ln.getNodeId());
        }

        /* Search for LN w/exact key. */
        location.index = location.bin.findEntry(location.lnKey,
                                                indicateIfExact,
                                                exactSearch);
        boolean match;
        if (findDeletedEntries) {
            match = (location.index >= 0 &&
                     (location.index & IN.EXACT_MATCH) != 0);
            location.index &= ~IN.EXACT_MATCH;
        } else {
            match = (location.index >= 0);
        }

        if (match) {
            location.childLsn = location.bin.getEntry(location.index).getLsn();
            return true;
        } else {
            return false;
        }
    }


    /**
     * Return a reference to the next BIN in a duplicate tree.
     *
     * @param child The BIN to find the next BIN for.  The bin is not latched.
     *
     * @param duplicateRoot The root of the duplicate tree that is being
     * iterated through or null if the root of the tree should be used.  It
     * should not be latched (if non-null) when this is called.
     *
     * @return The next BIN, or null if there are no more.  The returned node
     * is latched and the caller must release it.  If null is returned, the
     * argument bin remains latched.
     */
    public BIN getNextBin(BIN bin, DIN duplicateRoot)
        throws DatabaseException {

        return getNextBinInternal(duplicateRoot, bin, true);
    }

    /**
     * Return a reference to the previous BIN in a duplicate tree.
     *
     * @param child The BIN to find the previous BIN for.  The bin is not
     * latched.
     *
     * @param duplicateRoot The root of the duplicate tree that is being
     * iterated through or null if the root of the tree should be used.  It
     * should not be latched (if non-null) when this is called.
     *
     * @return The previous BIN, or null if there are no more.  The returned
     * node is latched and the caller must release it.  If null is returned,
     * the argument bin remains latched.
     */
    public BIN getPrevBin(BIN bin, DIN duplicateRoot)
        throws DatabaseException {

        return getNextBinInternal(duplicateRoot, bin, false);
    }

    /**
     * Helper routine for above two routines to iterate through
     * BIN's.
     */
    private BIN getNextBinInternal(DIN duplicateRoot,
				   BIN bin,
				   boolean forward)
        throws DatabaseException {

        /*
         * Use the right most key (for a forward progressing cursor) or the
         * left most key (for a backward progressing cursor) as the idkey.  The
         * reason is that the BIN may get split while finding the next BIN so
         * it's not safe to take the BIN's identifierKey entry.  If the BIN
         * gets splits, then the right (left) most key will still be on the
         * resultant node.  The exception to this is that if there are no
         * entries, we just use the identifier key.
         */
        Key idKey = null;
        int nEntries = bin.getNEntries();
        if (!forward || nEntries == 0) {
            idKey = (nEntries == 0) ?
                bin.getIdentifierKey() :
                bin.getEntry(0).getKey();
        } else {
            idKey = bin.getEntry(bin.getNEntries() - 1).getKey();
        }

        IN next = bin;
        next.latch();

        assert Latch.countLatchesHeld() == 1:
            Latch.latchesHeldToString();

        /* 
         * Ascend the tree until we find a level that still has nodes to the
         * right (or left if !forward) of the path that we're on.  If we reach
         * the root level, we're done. If we're searching within a duplicate
         * tree, stay within the tree.
         */
        while (true) {

            /* 
	     * Move up a level from where we are now and check to see if we
             * reached the top of the tree.
	     */
            IN parent = null;
            SearchResult result = null;
            if (duplicateRoot == null) {
                /* Operating on a regular tree -- get the parent. */
                result = getParentINForChildIN(next, true);
                if (result.exactParentFound) {
                    parent = result.parent;
                } else {
                    /* We've reached the root of the tree. */
                    assert (Latch.countLatchesHeld() == 0):
                        Latch.latchesHeldToString();
                    return null;
                }
            } else {
                /* We're operating on a duplicate tree, stay within the tree.*/
                if (next == duplicateRoot) {
                    /* We've reached the top of the dup tree. */
                    next.releaseLatch();
                    return null;
                } else {
                    result = getParentINForChildIN(next, true);
                    if (result.exactParentFound) {
                        parent = result.parent;
                    } else {
                        return null;
                    }
                }
            }

            assert (Latch.countLatchesHeld() == 1) :
                Latch.latchesHeldToString();

            /* 
             * Figure out which entry we are in the parent.  Add (subtract) 1
             * to move to the next (previous) one and check that we're still
             * pointing to a valid child.  Don't just use the result of the
             * parent.findEntry call in getParentNode, because we want to use
             * our explicitly chosen idKey.
             */
            int index = parent.findEntry(idKey, false, false);
            boolean moreEntriesThisBin = false;
            if (forward) {
                index++;
                if (index < parent.getNEntries()) {
                    moreEntriesThisBin = true;
                }
            } else {
                if (index > 0) {
                    moreEntriesThisBin = true;
                }
                index--;
            }

            if (moreEntriesThisBin) {

                /* 
                 * There are more entries to the right of the current path in
                 * parent.  Get the entry, and then descend down the left most
                 * path to a BIN.
                 */
		IN nextIN = (IN)
		    parent.getEntry(index).fetchTarget(database, parent);
                nextIN.latch();

                assert (Latch.countLatchesHeld() == 2):
                    Latch.latchesHeldToString();

                if (nextIN instanceof BIN) {
                    /* We landed at a leaf (i.e. a BIN). */
                    parent.releaseLatch();
		    if (treeStatsAccumulator != null) {
			nextIN.accumulateStats(treeStatsAccumulator);
		    }

                    return (BIN) nextIN;
                } else {

                    /* 
		     * We landed at an IN.  Descend down to the appropriate
		     * leaf (i.e. BIN) node.
		     */
                    IN ret = searchSubTree(nextIN, null,
                                           (forward ?
                                            SearchType.LEFT :
                                            SearchType.RIGHT),
                                           -1);
                    parent.releaseLatch();

                    assert Latch.countLatchesHeld() == 1:
                        Latch.latchesHeldToString();

                    if (ret instanceof BIN) {
                        return (BIN) ret;
                    } else {
                        throw new InconsistentNodeException
                            ("subtree did not have a BIN for leaf");
                    }
                }
            }
            next = parent;
        }
    }

    /**
     * Split the root of the tree.
     */
    private void splitRoot()
        throws DatabaseException {

        /* 
         * Create a new root IN, insert the current root IN into it, and then
         * call split.
         */
        EnvironmentImpl env = database.getDbEnvironment();
        LogManager logManager = env.getLogManager();
        INList inMemoryINs = env.getInMemoryINs();

        IN curRoot = null;
        curRoot = (IN) root.fetchTarget(database, null);
        curRoot.latch();

        /*
         * Make a new root IN, giving it an id key from the previous root.
         */
        Key rootIdKey = curRoot.getEntry(0).getKey();
        IN newRoot = new IN(database, rootIdKey, maxEntriesPerNode,
                            curRoot.getLevel() + 1);
        newRoot.setIsRoot(true);
        curRoot.setIsRoot(false);

        /*
         * Make the new root IN point to the old root IN. Log the old root
         * provisionally, because we modified it so it's not the root anymore,
         * then log the new root. We are guaranteed to be able to insert
         * entries, since we just made this root.
         */
        DbLsn curRootLsn = curRoot.logProvisional(logManager);
        boolean insertOk = newRoot.insertEntry
            (new ChildReference(curRoot, rootIdKey, curRootLsn));
        assert insertOk;

        DbLsn logLsn = newRoot.log(logManager);
        inMemoryINs.add(newRoot);

        /*
         * Make the tree's root reference point to this new node. Now the MapLN
         * is logically dirty, but the change hasn't been logged.  Be sure to
         * flush the MapLN if we ever evict the root.
         */
        root.setTarget(newRoot);
        root.setLsn(logLsn);
        DbLsn postSplitRootLsn =
	    curRoot.split(newRoot, 0, maxEntriesPerNode);
        root.setLsn(postSplitRootLsn);

        curRoot.releaseLatch();
        treeStats.nRootSplits++;
        traceSplitRoot(Level.FINE, TRACE_ROOT_SPLIT, newRoot, logLsn,
                       curRoot, curRootLsn);
    }

    /**
     * Search the tree, starting at the root.  Depending on search type either
     * search using key, or search all the way down the right or left sides.
     * Stop the search either when the bottom of the tree is reached, or a node
     * matching nid is found (see below) in which case that node's parent is
     * returned. 
     *
     * Preemptive splitting is not done during the search.
     *
     * @param key - the key to search for, or null if searchType is LEFT or
     * RIGHT.
     *
     * @param searchType - The type of tree search to perform.  NORMAL means
     * we're searching for key in the tree.  LEFT/RIGHT means we're descending
     * down the left or right side, resp.  DELETE means we're descending the
     * tree and will return the lowest node in the path that has > 1 entries.
     *
     * @param nid - The nodeid to search for in the tree.  If found, returns
     * its parent.  If the nodeid of the root is passed, null is returned.
     *
     * @return - the Node that matches the criteria, if any.  This is the node
     * that is farthest down the tree with a match.  Returns null if the root
     * is null.  Node is latched (unless it's null) and must be unlatched by
     * the caller.  Only IN's and BIN's are returned, not LN's.  In a NORMAL
     * search, It is the caller's responsibility to do the findEntry() call on
     * the key and BIN to locate the entry that matches key.  The return value
     * node is latched upon return and it is the caller's responsibility to
     * unlatch it.
     */
    public IN search(Key key,
                     SearchType searchType,
                     long nid)
        throws DatabaseException {

        IN rootIN = getRootIN();

        if (rootIN != null) {
            return searchSubTree(rootIN, key, searchType, nid);
        } else {
            return null;
        }
    }

    /**
     * Do a key based search, permitting pre-emptive splits. Returns the 
     * target node's parent..
     */
    private IN searchSplitsAllowed(Key key,
                                   long nid)
        throws DatabaseException {

        rootLatch.acquire();
        IN rootIN = null;
        try {
            if (root != null) {
                rootIN = (IN) root.fetchTarget(database, null);

                /* Check if root needs splitting. */
                if (rootIN.needsSplitting()) {
                    splitRoot();

                    /*
                     * We can't hold any latches while we lock.  If the root
                     * splits again between latch release and DbTree.db lock,
                     * no problem.  The latest root will still get written out.
                     */
                    rootLatch.release();
                    EnvironmentImpl env = database.getDbEnvironment();
                    env.getDbMapTree().modifyDbRoot(database);
                    rootLatch.acquire();
                    rootIN = (IN) root.fetchTarget(database, null);
                }
                rootIN.latch();
            }
        } finally {
            rootLatch.release();	
        }

        return searchSubTreeSplitsAllowed(rootIN, key, nid);
    }

    /**
     * Searches a portion of the tree starting at parent using key.  If during
     * the search a node matching a non-null nid argument is found, its parent
     * is returned.  If searchType is NORMAL, then key must be supplied to
     * guide the search.  If searchType is LEFT (or RIGHT), then the tree is
     * searched down the left (or right) side to find the first (or last) leaf,
     * respectively.  DELETE means we're descending the tree and will return
     * the lowest node in the path that has > 1 entries.
     * <p>
     * In the DELETE case, we verify that the BIN at the bottom of the subtree
     * has no entries in it because compression is about to occur.  If there
     * are entries in it (because entries were added between the time that the
     * BIN was turned into an empty BIN and the time that the compressor got
     * around to compressing it) then we unlatch everything before returning
     * and throw a NodeNotEmptyException.
     * <p>
     * Enters with parent latched, assuming it's not null.  Exits with the
     * return value latched, assuming it's not null.
     * <p>
     * @param parent - the root of the subtree to start the search at.  This
     * node should be latched by the caller and will be unlatched prior to
     * return.
     * 
     * @param key - the key to search for, unless searchType is LEFT or RIGHT
     *
     * @param searchType - NORMAL means search using key and, optionally, nid.
     *                     LEFT means find the first (leftmost) leaf
     *                     RIGHT means find the last (rightmost) leaf
     *                     DELETE means find the lowest node with > 1 entries.
     *
     * @param nid - The nodeid to search for in the tree.  If found, returns
     * its parent.  If the nodeid of the root is passed, null is returned.
     * Pass -1 if no nodeid based search is desired.
     *
     * @throws NodeNotEmptyException if DELETE was passed as a parameter and
     * the lowest level BIN at the bottom of the tree was not empty.
     *
     * @return - the node matching the argument criteria, or null.  The node is
     * latched and must be unlatched by the caller.  The parent argument and
     * any other nodes that are latched during the search are unlatched prior
     * to return.
     */
    public IN searchSubTree(IN parent,
			    Key key,
			    SearchType searchType,
                            long nid)
        throws DatabaseException {

        /* Return null if we're passed a null arg. */
        if (parent == null) {
            return null;
        }

        if ((searchType == SearchType.LEFT ||
             searchType == SearchType.RIGHT) &&
            key != null) {

            /* 
	     * If caller is asking for a right or left search, they * shouldn't
	     * be passing us a key.
	     */
            throw new IllegalArgumentException
                ("searchSubTree passed key and left/right search");
        }

        if (searchType == SearchType.DELETE &&
            (key == null || nid != -1 )) {

            /* 
	     * If caller is asking for a delete style search, they must * pass
	     * us a key, and can't pass a nid. 
	     */
            throw new IllegalArgumentException
                ("bad arguments for DELETE style search");
        }

        assert parent.getLatch().isOwner();

        if (parent.getNodeId() == nid) {
            parent.releaseLatch();
            return null;
        }

        int index;
        IN child = null;
                
        /* 
	 * If performing DELETE search, this saves the lowest IN in the path
	 * that has multiple entries.
         */
        IN lowestMultipleEntryIN = null;

        do {
	    if (treeStatsAccumulator != null) {
		parent.accumulateStats(treeStatsAccumulator);
	    }

            if (parent.getNEntries() == 0) {
                /* No more children, can't descend anymore. */
                return parent;
            } else if (searchType == SearchType.NORMAL) {
                /* Look for the entry matching key in the current node. */
                index = parent.findEntry(key, false, false);
            } else if (searchType == SearchType.LEFT) {
                /* Left search, always take the 0th entry. */
                index = 0;
            } else if (searchType == SearchType.RIGHT) {
                /* Right search, always take the highest entry. */
                index = parent.getNEntries() - 1;
            } else if (searchType == SearchType.DELETE) {

                /* 
		 * Same as SearchType.NORMAL, but also check to see
                 * if there are multiple entries in this node.
		 */
                index = parent.findEntry(key, false, false);
                if (parent.getNEntries() > 1) {
                    if (lowestMultipleEntryIN != null) {
                        lowestMultipleEntryIN.releaseLatch();
                    }
                    lowestMultipleEntryIN = parent;
                }
            } else {
                throw new IllegalArgumentException
                    ("Invalid value of searchType: " + searchType);
            }

            assert index >= 0;

            /* Get the child node that matches. */
            child = (IN) parent.getEntry(index).fetchTarget(database, parent);
            child.latch();

	    if (treeStatsAccumulator != null) {
		child.accumulateStats(treeStatsAccumulator);
	    }

            /* 
	     * If this child matches nid, then stop the search and return the
	     * parent.
	     */
            if (child.getNodeId() == nid) {
                child.releaseLatch();
                return parent;
            }

            if (parent != lowestMultipleEntryIN) {
                parent.releaseLatch();
            }

            /* Continue down a level */
            parent = child;
        } while (!(parent instanceof BIN));

        if (searchType == SearchType.DELETE) {
            boolean notEmpty = child.getNEntries() != 0;

            if (child != lowestMultipleEntryIN) {
                child.releaseLatch();
            }

            if (lowestMultipleEntryIN == null) {
                return null;
            }

            if (notEmpty || ((BIN) child).nCursors() > 0) {
                lowestMultipleEntryIN.releaseLatch();
                throw new NodeNotEmptyException();
            }

            return lowestMultipleEntryIN;
        } else {
            return child;
        }
    }

    /**
     * Search the portion of the tree starting at the parent, permitting 
     * preemptive splits. 
     */
    private IN searchSubTreeSplitsAllowed(IN parent,
                                          Key key,
                                          long nid)
        throws DatabaseException {
        if (parent != null) {

            /*
             * Search downward until we hit a node that needs a split. In
             * that case, retreat to the top of the tree and force splits
             * downward.
             */
            while (true) {
                try {
                    return searchSubTreeUntilSplit(parent, key, nid);
                } catch (SplitRequiredException e) {
                    forceSplit(parent, key);
                }
            }
        } else {
            return null;
        }
    }

    /**
     * Search the subtree, but throw an exception when we see a node 
     * that has to be split.
     */
    private IN searchSubTreeUntilSplit(IN parent,
                                       Key key,
                                       long nid)
        throws DatabaseException, SplitRequiredException {

        /* Return null if we're passed a null arg. */
        if (parent == null) {
            return null;
        }

        assert parent.getLatch().isOwner();

        if (parent.getNodeId() == nid) {
            parent.releaseLatch();
            return null;
        }

        int index;
        IN child = null;

        do {
            if (parent.getNEntries() == 0) {
                /* No more children, can't descend anymore. */
                return parent;
            } else {
                /* Look for the entry matching key in the current node. */
                index = parent.findEntry(key, false, false);
            }

            assert index >= 0;

            /* Get the child node that matches. */
            child = (IN) parent.getEntry(index).fetchTarget(database, parent);
            child.latch();

            /* Throw if we need to split. */
            if (child.needsSplitting()) {
                child.releaseLatch();
                parent.releaseLatch();
                throw new SplitRequiredException();
            }

            /* 
	     * If this child matches nid, then stop the search and return the
	     * parent.
	     */
            if (child.getNodeId() == nid) {
                child.releaseLatch();
                return parent;
            }

            /* Continue down a level */
            parent.releaseLatch();
            parent = child;
        } while (!(parent instanceof BIN));

        return parent;
    }

    /**
     * Do pre-emptive splitting in the subtree topped by the "parent" node.
     * Search down the tree until we get to the BIN level, and split any nodes
     * that fit the splittable requirement.
     * 
     * Note that more than one node in the path may be splittable. For example,
     * a tree might have a level2IN and a BIN that are both splittable, and 
     * would be encountered by the same insert operation.
     */
    private void forceSplit(IN parent,
                            Key key)
        throws DatabaseException {

        ArrayList nodeLadder = new ArrayList();

	boolean allLeftSideDescent = true;
	boolean allRightSideDescent = true;
        int index;
        IN child = null;
        IN originalParent = parent;

        /* We'll leave this method with the original parent latched. */
        originalParent.latch();

        /* 
         * Search downward to the BIN level, saving the information
         * needed to do a split if necessary.
         */
        do {
            if (parent.getNEntries() == 0) {
                /* No more children, can't descend anymore. */
                break;
            } else {
                /* Look for the entry matching key in the current node. */
                index = parent.findEntry(key, false, false);
                if (index != 0) {
                    allLeftSideDescent = false;
                }
                if (index != (parent.getNEntries() - 1)) {
                    allRightSideDescent = false;
                }
            }

            assert index >= 0;

            /* 
             * Get the child node that matches. We only need to work on
             * nodes in residence.
             */
            child = (IN) parent.getEntry(index).getTarget();
            if (child == null) {
                break;
            } else {
                child.latch();
                nodeLadder.add(new SplitInfo(parent, child, index));
            } 

            /* Continue down a level */
            parent = child;
        } while (!(parent instanceof BIN));

        boolean startedSplits = false;
        LogManager logManager =
            database.getDbEnvironment().getLogManager();

        /* 
         * Process the accumulated nodes from the bottom up. Split each
         * node if required. If the node should not split, we check
         * if there have been any splits on the ladder yet. If there
         * are none, we merely release the node, since there is no update.
         * If splits have started, we need to propagate new lsns upward,
         * so we log the node and update its parent.
         */

        /* Start this iterator at the end of the list. */
        ListIterator iter = nodeLadder.listIterator(nodeLadder.size());
        try {
            while (iter.hasPrevious()) {
                SplitInfo info = (SplitInfo) iter.previous();
                child = info.child;
                parent = info.parent;
                index = info.index;

                /* Opportunistically split the node if it is full. */
                if (child.needsSplitting()) {
                    if (allLeftSideDescent || allRightSideDescent) {
                        child.splitSpecial(parent, index,
                                           maxEntriesPerNode, key,
                                           allLeftSideDescent);
                    } else {
                        child.split(parent, index, maxEntriesPerNode);
                    }
                    startedSplits = true;
                } else {
                    if (startedSplits) {
                        DbLsn newLsn = child.log(logManager);
                        parent.updateEntry(index, newLsn);
                    } 
                }
                child.releaseLatch();
            }
        } finally {
            /* 
             * Unlatch any remaining children. There should only be remainders
             * in the event of an exception.
             */
            while (iter.hasPrevious()) {
                SplitInfo info = (SplitInfo) iter.previous();
                info.child.releaseLatch();
            }
        }
    }

    /**
     * Helper to obtain the root IN with proper root latching.
     */
    private IN getRootIN() 
        throws DatabaseException {
        rootLatch.acquire();
        IN rootIN = null;
        try {
            if (root != null) {
                rootIN = (IN) root.fetchTarget(database, null);
                rootIN.latch();
            }
            return rootIN;
        } finally {
            rootLatch.release();	
        }
    }

    /**
     * Inserts a new LN into the tree.
     * @param ln The LN to insert into the tree.
     * @param key Key value for the node
     * @param lsn LSN value for this previously logged ln;
     * @param allowDuplicates whether to allow duplicates to be inserted
     * @param cursor the cursor to update to point to the newly inserted
     * key/data pair, or null if no cursor should be updated.
     * @return true if LN was inserted, false if it was a duplicate
     * duplicate or if an attempt was made to insert a duplicate when
     * allowDuplicates was false.
     */
    public boolean insert(LN ln,
                          Key key,
                          boolean allowDuplicates,
                          CursorImpl cursor,
			  LockResult lnLock)
        throws DatabaseException {

        validateInsertArgs(allowDuplicates);

        EnvironmentImpl env = database.getDbEnvironment();
        LogManager logManager = env.getLogManager();
        INList inMemoryINs = env.getInMemoryINs();

        /* Find and latch the relevant BIN. */
        BIN bin = null;
        try {
            bin = findBinForInsert(key, logManager, inMemoryINs); 
            assert bin.getLatch().isOwner();
            
            /* Make a child reference as a candidate for insertion. */
            ChildReference newLNRef =
                new ChildReference(ln, key, new DbLsn(0,0));

	    /*
	     * If we're doing a put that is not a putCurrent, then the cursor
	     * passed in may not be pointing to bin (it was set to the bin that
	     * the search landed on which may be different than bin).  Set the
	     * bin correctly here so that adjustCursorsForInsert doesn't blow
	     * an assertion.  We'll finish the job by setting the index below.
	     */
	    cursor.setBIN(bin);
            int duplicateEntryIndex = bin.insertEntry1(newLNRef);
            if ((duplicateEntryIndex & IN.INSERT_SUCCESS) != 0) {
		lnLock.setAbortLsn(null, true, true);
                /* Not a duplicate. Insert was successful. */
                DbLsn newLsn = ln.log(env, database.getId(),
                                      key, null, cursor.getLocker());
                duplicateEntryIndex &= ~IN.INSERT_SUCCESS;

                /* 
		 * If called on behalf of a cursor, update it to point to the
		 * entry that has been successfully inserted.
		 */
		cursor.updateBin(bin, duplicateEntryIndex);
                bin.updateEntry(duplicateEntryIndex, newLsn);
                traceInsert(Level.FINER, env, bin, ln,
			    newLsn, duplicateEntryIndex);
            } else {

                /* 
		 * Entry may have been a duplicate. Insertion was not
		 * successful.
		 */
                duplicateEntryIndex &= ~IN.EXACT_MATCH;

                ChildReference ref = bin.getEntry(duplicateEntryIndex);
                LN currentLN = ln;
		int dupCount = -1;
		Node n = ref.fetchTargetIgnoreKnownDeleted(database, bin);
		if (n == null ||
		    n instanceof LN) {
		    currentLN = (LN) n;
		} else {
		    DIN duplicateRoot = (DIN) n;
		    currentLN = (DupCountLN)
			duplicateRoot.getDupCountLNRef().
			fetchTarget(database, duplicateRoot);
		    dupCount = ((DupCountLN) currentLN).getDupCount();
		}

		/*
		 * Must lock the LN before checking for deleted-ness.  (Note
		 * that DupCountLN's can't be deleted, but still have to be
		 * locked anyway). But to lock we must release the latch on the
		 * BIN.  During the time that the BIN is unlatched, a
		 * modification could happen.  Some changes can cause cursor
		 * adjustment and so we must read back the bin/index after we
		 * latch again.  Other changes can just change the "currentLN"
		 * out from underneath us.  If so, we have to go back and make
		 * sure we lock the correct one.  Note that changes to the
		 * currentLN can be caused in a thread that enters insert() and
		 * blocks on the readlock.  Then during that block, another
		 * thread enters insert with the same key and successfully
		 * inserts that data.  The same thread then continues on to
		 * delete that data that it just inserted (before the first
		 * thread returns).  When the first thread that is blocked
		 * finally gets the lock, the LN that currentLN refers to is
		 * long gone from ref.  This is the outer while loop.
		 * 
		 * Once we get the bin from the cursor, we latch it.  But the
		 * cursor could get adjusted between the getBIN() call and the
		 * latch acquisition.  So check again that it hasn't changed
		 * once it's under latch.  This is the inner while loop.
		 */
		cursor.updateBin(bin, duplicateEntryIndex);
		if (n != null) {
		    bin.releaseLatch();
		}
		Locker locker = cursor.getLocker();
		while (n != null) {

		    /*
		     * Write lock instead of read lock to avoid upgrade issues.
		     * There's a good chance that we'll need it for write
		     * anyway.
		     */
		    locker.writeLock(currentLN, database);
		    while (true) {
			bin = cursor.getBIN();
			bin.latch();
			if (bin != cursor.getBIN()) {
			    bin.releaseLatch();
			    continue;
			} else {
			    break;
			}
		    }
		    duplicateEntryIndex = cursor.getIndex();
		    ref = bin.getEntry(duplicateEntryIndex);
		    n = ref.fetchTargetIgnoreKnownDeleted(database, bin);
		    if (n == null) {
			currentLN = null;
			break;
		    }
		    if (n == currentLN ||
			dupCount != -1) {
			break;
		    } else {

			/*
			 * We should consider releasing the lock on currentLN
			 * here.  However, it may be been locked from a prior
			 * operation in this transaction.
			 */
			if (n instanceof LN) {
			    currentLN = (LN) n;
			    dupCount = -1;
			} else {
			    DIN duplicateRoot = (DIN) n;
			    currentLN = (DupCountLN)
				duplicateRoot.getDupCountLNRef().
				fetchTarget(database, duplicateRoot);
			    dupCount = ((DupCountLN) currentLN).getDupCount();
			}

			bin.releaseLatch();
		    }
		}

		/*
		 * If the ref is knownDeleted (DupCountLN's can't be deleted or
		 * knownDeleted), or the LN that it points to is not a
		 * DupCountLN and the data in it is deleted, then we substitute
		 * the argument LN for it.
		 *
		 * dupCount == -1 means there is no dup tree here.
		 */
		boolean isKnownDeleted = ref.isKnownDeleted();
                if (isKnownDeleted ||
		    (dupCount == -1 &&  // if currentLN is not a DupCountLN?
		     ((LN) ref.fetchTarget(database, bin)).isDeleted())) {

		    DbLsn existingAbortLsn = null;
		    if (currentLN != null) {
			existingAbortLsn =
			    locker.getAbortLsn(currentLN.getNodeId());
		    }
		    lnLock.setAbortLsn(existingAbortLsn, isKnownDeleted);

                    /* Current entry is a deleted entry. Replace it with LN. */
                    DbLsn newLsn = ln.log(env, database.getId(),
                                          key, null, cursor.getLocker());

                    /* 
		     * If called on behalf of a cursor, update it to point to
		     * the entry that has been successfully inserted.
		     */
                    bin.updateEntry(duplicateEntryIndex, ln, newLsn, key);
                    bin.clearKnownDeleted(duplicateEntryIndex);

		    /*
		     * We can't release the lock on currentLN that we took for
		     * read above because it might be held by this transaction
		     * on behalf of some other operation.
		     */
                    traceInsert(Level.FINER, env, bin, ln,
				newLsn, duplicateEntryIndex);

                    return true;
                }

		/*
		 * If there is an empty dup tree still around (perhaps because
		 * all of the entries in it have been deleted), then dupCount
		 * will be 0 and we can allow insertDuplicate to do it's work.
		 * It is possible to have allowDuplicates be false and
		 * duplicates (or a duplicate tree) to still be present because
		 * allowDuplicates gets passed as false from putNoOverwrite
		 * (this method only checks against the key and not the data
		 * portion).
		 */
                if (!allowDuplicates &&
		    dupCount != 0) {
                    return false;
                } else {

		    /* 
		     * We can add duplicates, or the dup tree is empty so we
		     * need to insert there.
		     */		       
		    cursor.updateBin(bin, duplicateEntryIndex);
                    return insertDuplicate(key, bin, ln, logManager,
					   inMemoryINs, cursor, lnLock);
                }
            }
        } finally {
            if (bin != null &&
		bin.getLatch().isOwner()) {
                bin.releaseLatch();
            }
        }

        return true;
    }

    /**
     * Return true if duplicate inserted successfully, false if it was a
     * duplicate duplicate.
     */
    private boolean insertDuplicate(Key key,
				    BIN bin,
                                    LN newLN,
                                    LogManager logManager,
                                    INList inMemoryINs,
                                    CursorImpl cursor,
				    LockResult lnLock)
        throws DatabaseException {

        EnvironmentImpl env = database.getDbEnvironment();
	int index = cursor.getIndex();
        boolean successfulInsert = false;
        int latchCount = 0;
        /* Intentional side effect to set latch count for debugging. */
        assert ((latchCount = Latch.countLatchesHeld()) > -1); 

        DIN duplicateRoot = null;
        Node n = bin.fetchTarget(index);

        if (n instanceof DIN) {
            DBIN duplicateBin = null;

            /*
             * A duplicate tree exists.  Find the relevant DBIN and insert the
             * new entry into it.
             */
            try {
                duplicateRoot = (DIN) n;
                duplicateRoot.latch();

                if (maybeSplitDuplicateRoot(bin, index)) {
                    duplicateRoot = (DIN) bin.fetchTarget(index);
                    /* It's already latched. */
                }

                /* 
                 * Search the duplicate tree for the right place to insert this
                 * new record. Releases the latch on duplicateRoot.
                 */
                Key newLNKey = new Key(newLN.getData());
		duplicateBin = (DBIN)
                    searchSubTreeSplitsAllowed(duplicateRoot, newLNKey, -1);

                /* 
                 * Try insert a new reference object. If successful, we'll log
                 * the ln and update the lsn in the reference.
                 */
                ChildReference newLNRef = 
                    new ChildReference(newLN, newLNKey, DbLsn.NULL_LSN);
                                       
                int duplicateEntryIndex = duplicateBin.insertEntry1(newLNRef);
                if ((duplicateEntryIndex & IN.INSERT_SUCCESS) != 0) {

                    /* 
                     * Update the cursor to point to the entry that has been
                     * successfully inserted.
                     */
		    lnLock.setAbortLsn(null, true, true);

		    duplicateEntryIndex &= ~IN.INSERT_SUCCESS;
		    cursor.updateDBin(duplicateBin, duplicateEntryIndex);
                    DbLsn newLsn = newLN.log(env,
                                             database.getId(),
                                             key,
                                             null,
                                             cursor.getLocker());
                    newLNRef.setLsn(newLsn);

                    traceInsertDuplicate(Level.FINER,
                                         database.getDbEnvironment(),
                                         duplicateBin, newLN, newLsn, bin);
                    successfulInsert = true;
                } else {

                    /* 
                     * The insert was not successful. Either this is a
                     * duplicate duplicate or there is an existing entry but
                     * that entry is deleted.
                     */
                    duplicateEntryIndex &= ~IN.EXACT_MATCH;
                    ChildReference targetRef =
                        duplicateBin.getEntry(duplicateEntryIndex);
                    LN currentLN = (LN) targetRef.fetchTargetIgnoreKnownDeleted
                            (database, duplicateBin);

		    /*
		     * Must lock the LN before checking for deleted-ness.  *
                     * But to lock we must release the latch on the DBIN.  *
                     * During the time that the DBIN is unlatched, a *
                     * modification could happen.  Any change will cause *
                     * cursor adjustment.  So read back the dbin/dindex * after
                     * we latch again.
		     *
		     * See the comment at the similar piece of code in *
                     * insert() above.
		     */
		    cursor.updateDBin(duplicateBin, duplicateEntryIndex);
		    duplicateBin.releaseLatch();
		    Locker locker = cursor.getLocker();
                    /* targetRef is where we hope to insert this LN. */
                    targetRef = null; 
		    while (currentLN != null) {
			locker.readLock(currentLN);
			while (true) {
			    duplicateBin = cursor.getDupBIN();
			    duplicateBin.latch();
			    if (duplicateBin != cursor.getDupBIN()) {
				duplicateBin.releaseLatch();
				continue;
			    } else {
				break;
			    }
			}
			duplicateEntryIndex = cursor.getDupIndex();
			targetRef = duplicateBin.getEntry(duplicateEntryIndex);
			n = targetRef.fetchTargetIgnoreKnownDeleted
			    (database, duplicateBin);
			if (n == null) {
			    currentLN = null;
			    break;
			}
			if (n == currentLN) {
			    break;
			} else {
			    duplicateBin.releaseLatch();
			    currentLN = (LN) n;
			}
		    }

		    boolean isKnownDeleted = targetRef.isKnownDeleted();
                    if (isKnownDeleted || currentLN.isDeleted()) {

                        /* 
			 * Existing entry is deleted.  Replace it.  If called
                         * on behalf of a cursor, update it to point to the
                         * entry that has been successfully inserted.
			 */
			cursor.updateDBin(duplicateBin, duplicateEntryIndex);
			DbLsn existingAbortLsn = null;
			if (currentLN != null) {
			    existingAbortLsn =
				locker.getAbortLsn(currentLN.getNodeId());
			}
			lnLock.setAbortLsn(existingAbortLsn, isKnownDeleted);

                        DbLsn newLsn = newLN.log(env, database.getId(),
                                                 key, null,
                                                 cursor.getLocker());

                        duplicateBin.updateEntry
                            (duplicateEntryIndex, newLN, newLsn, newLNKey);
                        duplicateBin.clearKnownDeleted(duplicateEntryIndex);
                        traceInsertDuplicate(Level.FINER,
                                             database.getDbEnvironment(),
                                             duplicateBin, newLN, newLsn, bin);
                        successfulInsert = true;
                    } else {
                        /* Duplicate duplicate. */
                        successfulInsert = false;
                    }
                }

		if (successfulInsert) {
                    incrementDuplicateCount(env, cursor, key);
		}
            } finally {
                if (duplicateBin != null) {
                    if (duplicateBin.getLatch().isOwner()) {
                        duplicateBin.releaseLatch();
                    }
                }
		if (duplicateRoot.getLatch().isOwner()) {
		    duplicateRoot.releaseLatch();
		}
            }
        } else if (n instanceof LN) {

            /*
             * There is no duplicate tree yet.  Mutate the current BIN/LN pair
             * into a BIN/DupCountLN/DIN/DBIN/LN duplicate tree.  Log the new
             * entries.
             */
            try {
		lnLock.setAbortLsn(null, true, true);
                duplicateRoot =
                    createDuplicateTree(key, logManager, inMemoryINs,
					newLN, cursor);
            } finally {
                if (duplicateRoot != null) {
                    duplicateRoot.releaseLatch();
                    successfulInsert = true;
                } else {
                    successfulInsert = false;
                }
            }
        } else {
            throw new InconsistentNodeException
                ("neither LN or DIN found in BIN");
        }

        assert (Latch.countLatchesHeld() == latchCount):
            "latchCount=" + latchCount +
            " held=" + Latch.countLatchesHeld();
        return successfulInsert;
    }

    /**
     * Increment the count of records in a duplicate tree.
     * @param cursor current cursor
     */
    private void incrementDuplicateCount(EnvironmentImpl env,
                                         CursorImpl cursor,
                                         Key key)
        throws DatabaseException {

        /*
         * The duplicate count is stored in a special LN hanging off the root
         * DIN of the duplicate tree. To access and update the count, we must
         * do the usual latch/unlatch/lock LN/latch again pattern.
	 *
         * Navigate through the BIN to the root of the duplicate tree to the
         * DupCountLN.
         */
        DIN duplicateRoot = getLatchedDuplicateTreeRoot(cursor);
        DupCountLN dupCountLN = duplicateRoot.getDupCountLN();

        /* Write lock the DupCountLN */
        duplicateRoot.releaseLatch();
        cursor.releaseBINs();
        LockResult lockResult =
            cursor.getLocker().writeLock(dupCountLN, database);
        LockGrantType lockStatus = lockResult.getLockGrant();

        /* 
         * Relatch the BIN and the DIN which is the root of the duplicate tree.
         * We must re-establish our references to the owning duplicate root
         * DIN, because other threads could have changed the duplicate tree
         * while we were waiting for the latch.
         */
        cursor.latchBIN();
        duplicateRoot = getLatchedDuplicateTreeRoot(cursor);
        duplicateRoot.incrementDuplicateCount(env, lockResult,
                                              key, cursor.getLocker());
    }

    /*
     * Get the DIN root of the duplicate tree at this cursor's current
     * position.
     */
    private DIN getLatchedDuplicateTreeRoot(CursorImpl cursor) 
        throws DatabaseException {

        int index = cursor.getIndex();
        BIN bin = cursor.getBIN();
        DIN duplicateRoot = (DIN)
            bin.getEntry(index).fetchTarget(database, bin);
        duplicateRoot.latch();
        return duplicateRoot;
    }


    /**
     * Check if the duplicate root needs to be split.  The current duplicate
     * root is latched.  Exit with the new root (even if it's unchanged)
     * latched and the old root (unless the root is unchanged) unlatched.
     * 
     * @param bin the BIN containing the duplicate root.
     * @param index the index of the duplicate root in bin.
     * @return true if the duplicate root was split.
     */
    private boolean maybeSplitDuplicateRoot(BIN bin,
                                            int index)
        throws DatabaseException {

        ChildReference ref = bin.getEntry(index);
        DIN curRoot = (DIN) ref.fetchTarget(database, bin);

        if (curRoot.needsSplitting()) {

            EnvironmentImpl env = database.getDbEnvironment();
            LogManager logManager = env.getLogManager();
            INList inMemoryINs = env.getInMemoryINs();

            /* 
             * Make a new root DIN, giving it an id key from the previous root.
             */
            Key rootIdKey = curRoot.getEntry(0).getKey();
            DIN newRoot = new DIN(database,
                                  rootIdKey,
                                  maxEntriesPerNode,
                                  curRoot.getDupKey(),
                                  curRoot.getDupCountLNRef(),
                                  curRoot.getLevel() + 1);
            newRoot.latch();
            newRoot.setIsRoot(true);
            curRoot.setDupCountLN(null);
            curRoot.setIsRoot(false);

            /* 
             * Make the new root DIN point to the old root DIN, and then
             * log. We should be able to insert into the root because the root
             * is newly created.
             */
            DbLsn curRootLsn = curRoot.logProvisional(logManager);
            boolean insertOk = newRoot.insertEntry
                (new ChildReference(curRoot, rootIdKey, ref.getLsn()));
            assert insertOk;

            DbLsn logLsn = newRoot.log(logManager);
            inMemoryINs.add(newRoot);
            
            bin.updateEntry(index, newRoot, logLsn);
            curRoot.split(newRoot, 0, maxEntriesPerNode);

            curRoot.releaseLatch();
            traceSplitRoot(Level.FINE, 
			   TRACE_DUP_ROOT_SPLIT,
			   newRoot, logLsn,
			   curRoot, curRootLsn);
            return true;
        } else {
            return false;
        }
    }

    /**
     * Convert an existing BIN entry from a single (non-duplicate) LN to a new
     * DIN/DupCountLN->DBIN->LN subtree.
     *
     * @param key the key of the entry which will become the duplicate key
     * for the duplicate subtree.
     * @param logManager the logManager
     * @param inMemoryINs the in memory IN list
     * @param newLN the new record to be inserted
     * @param cursor points to the target position for this new dup tree.
     * @return the new duplicate subtree root (a DIN).  It is latched
     * when it is returned and the caller should unlatch it.  If new entry
     * to be inserted is a duplicate of the existing LN, null is returned.
     */
    private DIN createDuplicateTree(Key key,
                                    LogManager logManager,
                                    INList inMemoryINs,
                                    LN newLN,
                                    CursorImpl cursor)
        throws DatabaseException {

        EnvironmentImpl env = database.getDbEnvironment();
        DIN duplicateRoot = null;
        DBIN duplicateBin = null;
        BIN bin = cursor.getBIN();
        int index = cursor.getIndex();
        LN existingLN = (LN) bin.getEntry(index).fetchTarget(database, bin);
        Key existingKey = new Key(existingLN.getData());
        Key newLNKey = new Key(newLN.getData());

        /* Check for duplicate duplicates. */
        Comparator userComparisonFcn = database.getDuplicateComparator();
        boolean keysEqual =
            (userComparisonFcn == null ?
             newLNKey.compareTo(existingKey) :
             userComparisonFcn.compare(newLNKey.getKey(),
                                        existingKey.getKey())) == 0;
        if (keysEqual) {
            return null;
        }

        /* 
         * Replace the existing LN with a duplicate tree. 
         * 
         * Once we create a dup tree, we don't revert back to the LN.  * Create
         * a DupCountLN to hold the count for this dup tree. Since we don't
         * roll back the internal nodes of a duplicate tree, we need to create
         * a pre-transaction version of the DupCountLN. This version must hold
         * a count of either 0 or 1, depending on whether the current
         * transaction created the exising lN or not. If the former, the count
         * must roll back to 0, if the latter, the count must roll back to 1.
         *
         * Note that we are logging a sequence of nodes and must make sure the
         * log can be correctly recovered even if the entire sequence doesn't
         * make it to the log. We need to make all children provisional to the
         * DIN. This works:
         *
         * Entry 1: (provisional) DupCountLN (first version)
         * Entry 2: (provisional) DupBIN 
         * Entry 3: DIN
         * Entry 4: DupCountLN (second version, incorporating the new count.
         *           This can't be provisional because we need to possibly
         *            roll it back.)
         * Entry 5: new LN.
         * See [SR #10203] for a description of the bug that existed before
         * this change.
         */

        /* Create the first version of DupCountLN and log it. (Entry 1). */
        Locker locker = cursor.getLocker();
        int startingCount = locker.createdNode(existingLN.getNodeId()) ? 0 : 1;
        DupCountLN dupCountLN = new DupCountLN(startingCount);
        DbLsn firstDupCountLNLsn =
            dupCountLN.logProvisional(env, database.getId(), key, null);

        /* Make the duplicate root and DBIN. */
        duplicateRoot = new DIN(database,
                                existingKey,                   // idkey
                                maxEntriesPerNode,
                                key,                           // dup key
                                new ChildReference
                                (dupCountLN, key, firstDupCountLNLsn),
                                2);                            // level
        duplicateRoot.latch();
        duplicateRoot.setIsRoot(true);

        duplicateBin = new DBIN(database,
                                existingKey,                   // idkey
                                maxEntriesPerNode,
                                key,                           // dup key
                                1);                            // level
        duplicateBin.latch();

        /* 
         * Attach the existing LN child to the duplicate BIN. Since this is a
         * newly created BIN, insertEntry will be successful.
         */
        ChildReference newExistingLNRef =
            new ChildReference(existingLN,
                               existingKey,
                               bin.getEntry(index).getLsn());

        boolean insertOk = duplicateBin.insertEntry(newExistingLNRef);
        assert insertOk;

        /* Entry 2: DBIN. */
        DbLsn dbinLsn = duplicateBin.logProvisional(logManager);
        inMemoryINs.add(duplicateBin);
        
        /* Attach the duplicate BIN to the duplicate IN root. */
        duplicateRoot.setEntry
            (0, new ChildReference(duplicateBin,
                                   duplicateBin.getEntry(0).getKey(),
                                   dbinLsn));

        /* Entry 3:  DIN */
        DbLsn dinLsn = duplicateRoot.log(logManager);
        inMemoryINs.add(duplicateRoot);

        /*
         * Now that the DIN is logged, we've created a duplicate tree that
         * holds the single, preexisting LN. We can safely create the non
         * provisional LNs that pertain to this insert -- the new LN and the
         * new DupCountLN.
         *
         * We request a lock while holding latches which is usually forbidden,
         * but safe in this case since we know it will be immediately granted
         * (we just created dupCountLN above).
         */
	LockResult lockResult = locker.writeLock(dupCountLN, database);
	LockGrantType lockStatus = lockResult.getLockGrant();
	lockResult.setAbortLsn(firstDupCountLNLsn, false);

        dupCountLN.setDupCount(2);
        DbLsn dupCountLsn = dupCountLN.log(env, database.getId(), key,
                                           firstDupCountLNLsn, locker);
        duplicateRoot.updateDupCountLNRef(dupCountLsn);
        
        /* Add the newly created LN. */
        DbLsn newLsn = newLN.log(env, database.getId(), key, null, locker);
        int duplicateEntryIndex = 
            duplicateBin.insertEntry1(new ChildReference(newLN,
                                                         newLNKey,
                                                         newLsn));
	duplicateEntryIndex &= ~IN.INSERT_SUCCESS;
	cursor.updateDBin(duplicateBin, duplicateEntryIndex);
        duplicateBin.releaseLatch();

        /* 
         * Update the "regular" BIN to point to the new duplicate tree instead
         * of the existing LN.
         */
        bin.updateEntry(index, duplicateRoot, dinLsn);
        traceMutate(Level.FINE, bin, 
                    existingLN, newLN, newLsn, dupCountLN,
                    dupCountLsn, duplicateRoot,
                    dinLsn, duplicateBin, dbinLsn);
        return duplicateRoot;
    }

    /**
     * Validate args passed to insert.  Presently this just means making sure
     * that if they say duplicates are allowed that the database supports
     * duplicates.
     */
    private void validateInsertArgs(boolean allowDuplicates)
        throws DatabaseException {
        if (allowDuplicates && !database.getSortedDuplicates()) {
            throw new DatabaseException
                ("allowDuplicates passed to insert but database doesn't " +
                 "have allow duplicates set.");
        }
    }

    /*
     * Find the BIN that is relevant to the insert.  If the tree doesn't exist
     * yet, then create the first IN and BIN.
     * @return the BIN that was found or created and
     * return it latched.
     */
    private BIN findBinForInsert(Key key,
                                 LogManager logManager,
                                 INList inMemoryINs)
        throws DatabaseException {

	BIN bin;
        try {
	    DbLsn logLsn;

	    /* 
	     * We may have to try several times because of a small
	     * timing window, explained below.
	     */
	    while (true) {
		rootLatch.acquire();
		if (root == null) {

		    /* 
		     * This is an empty tree, either because it's brand new
		     * tree or because everything in it was deleted. Create an
		     * IN and a BIN.  We could latch the rootIN here, but
		     * there's no reason to since we're just creating the
		     * initial tree and we have the rootLatch held. Log the
		     * nodes as soon as they're created, but remember that
		     * referred-to children must come before any references to
		     * their lsns.
		     */
		    /* Log the root right away. */
		    IN rootIN = new IN(database, key, maxEntriesPerNode, 2);
		    rootIN.setIsRoot(true);
		    logLsn = rootIN.log(logManager);
		    root = new ChildReference(rootIN,
					      new Key(new byte[0]), 
					      logLsn);

		    /* First bin in the tree, log right away. */
		    bin = new BIN(database, key, maxEntriesPerNode, 1);
		    bin.latch();
		    logLsn = bin.log(logManager);

		    /* 
		     * Guaranteed to be okay since the root was newly created.
		     */
		    boolean insertOk = rootIN.insertEntry
			(new ChildReference(bin, key, logLsn));
		    assert insertOk;

		    /* Add the new nodes to the in memory list. */
		    inMemoryINs.add(bin);
		    inMemoryINs.add(rootIN);
		    rootLatch.release();
		    break;
		} else {
		    rootLatch.release();

		    /* 
		     * Ok, there's a tree here, so search for where we should
		     * insert. However, note that a window exists after we
		     * release the root latch. We release the latch because the
		     * search method expects to take the latch. After the
		     * release and before search, the INCompressor may come in
		     * and delete the entire tree, so search may return with a
		     * null.
		     */
		    IN in = searchSplitsAllowed(key, -1);
		    if (in == null) {
			/* The tree was deleted by the INCompressor. */
			continue;
		    } else {
			/* search() found a BIN where this key belongs. */
			bin = (BIN) in;
			break;
		    } 
		} 
	    }
        } finally {
            if (rootLatch.isOwner()) {
                rootLatch.release();
            }
        }

        return bin;
    }

    /*
     * Given a subtree root (an IN), remove it and all of its children from the
     * in memory IN list.  The INList latch is held upon entering.
     */
    private void removeSubtreeFromINList(INList inMemoryINs, IN subtreeRoot)
        throws DatabaseException {

        subtreeRoot.removeFromINList(inMemoryINs);
        Tracer.trace(Level.FINE, database.getDbEnvironment(),
		     "RemovedSubtreeFromInList: subtreeRoot = " +
		     subtreeRoot.getNodeId());
    }

    /*
     * Logging support
     */

    /**
     * @see LogWritable#getLogSize
     */
    public int getLogSize() {
        int size = LogUtils.getBooleanLogSize();  // root exists?
        if (root != null) {      
            size += root.getLogSize();              // root
        }
        return size;
    }

    /**
     * @see LogWritable#writeToLog
     */
    public void writeToLog(ByteBuffer logBuffer) {
        LogUtils.writeBoolean(logBuffer, (root != null));
        if (root != null) {
            root.writeToLog(logBuffer);
        }
    }

    /**
     * @see LogReadable#readFromLog
     */
    public void readFromLog(ByteBuffer itemBuffer) {
        boolean rootExists = LogUtils.readBoolean(itemBuffer);
        if (rootExists) {
            root = new ChildReference();
            root.readFromLog(itemBuffer);
        }
    }

    /**
     * @see LogReadable#dumpLog
     */
    public void dumpLog(StringBuffer sb, boolean verbose) {
        sb.append("<root>");
        if (root != null) {
            root.dumpLog(sb, verbose);
        }
        sb.append("</root>");
    }

    /**
     * @see LogReadable#isTransactional
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
     * rebuildINList is used by recovery to add all the resident nodes to the
     * IN list.
     */
    public void rebuildINList()
        throws DatabaseException {

        INList inMemoryList = database.getDbEnvironment().getInMemoryINs();

        if (root != null) {
            rootLatch.acquire();
            try {
                Node rootIN = root.getTarget();
                if (rootIN != null) {
                    rootIN.rebuildINList(inMemoryList);
                }
            } finally {
                rootLatch.release();
            }
        }
    }

    /*
     * Debugging stuff.
     */
    public void dump()
        throws DatabaseException {

        System.out.println(dumpString(0));
    }   

    public String dumpString(int nSpaces)
        throws DatabaseException {

        StringBuffer sb = new StringBuffer();
        sb.append(TreeUtils.indent(nSpaces));
        sb.append("<tree>");
        sb.append('\n');
        if (root != null) {
            sb.append(root.getLsn().dumpString(nSpaces));
            sb.append('\n');
            IN rootIN = (IN) root.getTarget();
            if (rootIN == null) {
                sb.append("<in/>");
            } else {
                sb.append(rootIN.toString());
            }
            sb.append('\n');
        }
        sb.append(TreeUtils.indent(nSpaces));
        sb.append("</tree>");
        return sb.toString();
    }   

    void dumpKeys()
        throws DatabaseException {

        Node nextBin = getFirstNode();

        while (nextBin != null) {
            BIN bin = (BIN) nextBin;
            System.out.println("-----------");
            bin.dumpKeys();
            nextBin = getNextBin(bin, null);
        }
        System.out.println("-----------");
        return;
    }

    /**
     * Unit test support to validate subtree pruning. Didn't want
     * to make root access public.
     */
    boolean validateDelete(int index)
        throws DatabaseException {

        rootLatch.acquire();
        try {
            IN rootIN = (IN) root.fetchTarget(database, null);
            return rootIN.validateSubtreeBeforeDelete(index);
        } finally {
            rootLatch.release();
        }
    }

    /**
     * Send trace messages to the java.util.logger. Don't rely on the logger
     * alone to conditionalize whether we send this message, we don't even want
     * to construct the message if the level is not enabled.
     */
    private void traceSplitRoot(Level level,
                                String splitType,
                                IN newRoot,
                                DbLsn newRootLsn,
                                IN oldRoot,
                                DbLsn oldRootLsn) {
        Logger logger = database.getDbEnvironment().getLogger();
        if (logger.isLoggable(level)) {
            StringBuffer sb = new StringBuffer();
            sb.append(splitType);
            sb.append(" newRoot=").append(newRoot.getNodeId());
            sb.append(" newRootLsn=").append(newRootLsn.getNoFormatString());
            sb.append(" oldRoot=").append(oldRoot.getNodeId());
            sb.append(" oldRootLsn=").append(oldRootLsn.getNoFormatString());
            logger.log(level, sb.toString());
        }
    }

    /**
     * Send trace messages to the java.util.logger. Don't rely on the logger
     * alone to conditionalize whether we send this message, we don't even want
     * to construct the message if the level is not enabled.
     */
    private void traceMutate(Level level,
                             BIN theBin,
                             LN existingLn,
                             LN newLn,
                             DbLsn newLsn,
                             DupCountLN dupCountLN,
                             DbLsn dupRootLsn,
                             DIN duplicateDIN,
                             DbLsn ddinLsn,
                             DBIN duplicateBin,
                             DbLsn dbinLsn) {
        Logger logger = database.getDbEnvironment().getLogger();
        if (logger.isLoggable(level)) {
            StringBuffer sb = new StringBuffer();
            sb.append(TRACE_MUTATE);
            sb.append(" existingLn=");
            sb.append(existingLn.getNodeId());
            sb.append(" newLn=");
            sb.append(newLn.getNodeId());
            sb.append(" newLnLsn=");
            sb.append(newLsn.getNoFormatString());
            sb.append(" dupCountLN=");
            sb.append(dupCountLN.getNodeId());
            sb.append(" dupRootLsn=");
            sb.append(dupRootLsn.getNoFormatString());
            sb.append(" rootdin=");
            sb.append(duplicateDIN.getNodeId());
            sb.append(" ddinLsn=");
            sb.append(ddinLsn.getNoFormatString());
            sb.append(" dbin=");
            sb.append(duplicateBin.getNodeId());
            sb.append(" dbinLsn=");
            sb.append(dbinLsn.getNoFormatString());
            sb.append(" bin=");
            sb.append(theBin.getNodeId());
	
            logger.log(level, sb.toString());
        }
    }

    /**
     * Send trace messages to the java.util.logger. Don't rely on the logger
     * alone to conditionalize whether we send this message, we don't even want
     * to construct the message if the level is not enabled.
     */
    private void traceInsert(Level level,
                             EnvironmentImpl env,
                             BIN insertingBin,
                             LN ln,
                             DbLsn lnLsn,
			     int index) {
        Logger logger = env.getLogger();
        if (logger.isLoggable(level)) {
            StringBuffer sb = new StringBuffer();
            sb.append(TRACE_INSERT);
            sb.append(" bin=");
            sb.append(insertingBin.getNodeId());
            sb.append(" ln=");
            sb.append(ln.getNodeId());
            sb.append(" lnLsn=");
            sb.append(lnLsn.getNoFormatString());
            sb.append(" index=");
	    sb.append(index);
	
            logger.log(level, sb.toString());
        }
    }

    /**
     * Send trace messages to the java.util.logger. Don't rely on the logger
     * alone to conditionalize whether we send this message, we don't even want
     * to construct the message if the level is not enabled.
     */
    private void traceInsertDuplicate(Level level,
                                      EnvironmentImpl env,
                                      BIN insertingDBin,
                                      LN ln,
                                      DbLsn lnLsn,
                                      BIN bin) {
        Logger logger = env.getLogger();
        if (logger.isLoggable(level)) {
            StringBuffer sb = new StringBuffer();
            sb.append(TRACE_INSERT_DUPLICATE);
            sb.append(" dbin=");
            sb.append(insertingDBin.getNodeId());
            sb.append(" bin=");
            sb.append(bin.getNodeId());
            sb.append(" ln=");
            sb.append(ln.getNodeId());
            sb.append(" lnLsn=");
            sb.append(lnLsn.getNoFormatString());
	
            logger.log(level, sb.toString());
        }
    }

    static private class SplitInfo {
        IN parent;
        IN child;
        int index;

        SplitInfo(IN parent, IN child, int index) {
            this.parent = parent;
            this.child = child;
            this.index = index;
        }
    }
}
