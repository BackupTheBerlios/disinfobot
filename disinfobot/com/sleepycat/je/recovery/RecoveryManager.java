/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: RecoveryManager.java,v 1.1 2004/11/22 18:27:59 kate Exp $
*/

package com.sleepycat.je.recovery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.cleaner.TrackedFileSummary;
import com.sleepycat.je.cleaner.UtilizationTracker;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.DbTree;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.latch.Latch;
import com.sleepycat.je.log.CheckpointFileReader;
import com.sleepycat.je.log.FileManager;
import com.sleepycat.je.log.INFileReader;
import com.sleepycat.je.log.LNFileReader;
import com.sleepycat.je.log.LastFileReader;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.UtilizationFileReader;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.ChildReference;
import com.sleepycat.je.tree.DIN;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.tree.Key;
import com.sleepycat.je.tree.LN;
import com.sleepycat.je.tree.Node;
import com.sleepycat.je.tree.SearchResult;
import com.sleepycat.je.tree.TrackingInfo;
import com.sleepycat.je.tree.Tree;
import com.sleepycat.je.tree.TreeLocation;
import com.sleepycat.je.tree.WithRootLatched;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.Tracer;

/**
 * 
 */
public class RecoveryManager {
    private static final String TRACE_DUP_ROOT_REPLACE =
        "DupRootRecover:";
    private static final String TRACE_LN_REDO = "LNRedo:";
    private static final String TRACE_LN_UNDO = "LNUndo";
    private static final String TRACE_IN_REPLACE = "INRecover:";
    private static final String TRACE_ROOT_REPLACE = "RootRecover:";
    private static final String TRACE_IN_DEL_REPLAY = "INDelReplay:";

    private static final int CLEAR_INCREMENT = 100;

    private EnvironmentImpl env;
    private int readBufferSize;
    private RecoveryInfo info;       // stat info
    private Set committedTxnIds;     // committed txns
    private Set inListRebuildDbIds;  // dbs for which we have to rebuild the
                                     // in memory IN list.

    private Level detailedTraceLevel; // level value for detailed trace msgs
    private Map fileSummaryLsns;      // file number -> DbLsn of FileSummaryLN
    private int inListClearCounter;   // governs intermediate IN list clearing

    /**
     * Make a recovery manager
     */
    public RecoveryManager(EnvironmentImpl env)
        throws DatabaseException {

        this.env = env;
        DbConfigManager cm = env.getConfigManager();
        readBufferSize =
            cm.getInt(EnvironmentParams.LOG_ITERATOR_READ_SIZE);
        committedTxnIds = new HashSet();
        inListRebuildDbIds = new HashSet();
        fileSummaryLsns = new HashMap();

        /* 
         * Figure out the level to use for detailed trace messages, by choosing
         * the more verbose of the recovery manager's trace setting vs the
         * general trace setting.
         */
        detailedTraceLevel =
            Tracer.parseLevel(env,
                              EnvironmentParams.JE_LOGGING_LEVEL_RECOVERY);
    }
    
    /**
     * Look for an existing log and use it to create an in memory structure for
     * accessing existing databases. The file manager and logging system are
     * only available after recovery.
     * @return RecoveryInfo statistics about the recovery process.
     */
    public RecoveryInfo recover(boolean readOnly)
        throws DatabaseException {

        info = new RecoveryInfo();

        try {
            FileManager fileManager = env.getFileManager();
            if (fileManager.filesExist()) {

                /* 
                 * Establish the location of the end of the log. After this, we
                 * can write to the log. No Tracer calls are allowed until
                 * after this point is established in the log.
                 */
                DbLsn checkpointLsn = findEndOfLog(readOnly);
                Tracer.trace(Level.INFO, env,
                             "Recovery underway, found end of log");
        
                /*
                 * Establish the location of the root, the last checkpoint, and
                 * the first active lsn by finding the last checkpoint.
                 */
                findLastCheckpoint(checkpointLsn);
                Tracer.trace(Level.INFO, env,
                             "Recovery checkpoint search, " +
                             info);

                /* Read in the root. */
                env.readMapTreeFromLog(info.useRootLsn);

                /* Rebuild the in memory tree from the log. */
                buildTree();
            } else {

                /*
                 * Nothing more to be done. Enable publishing of debug log
                 * messages to the database log.
                 */
                env.enableDebugLoggingToDbLog();
                Tracer.trace(Level.INFO, env, "Recovery w/no files.");
                env.logMapTreeRoot();
            }

            /*
             * At this point, we've recovered (or there were no log files at
             * all. Write a checkpoint into the log. Don't allow deltas,
             * because the delta-determining scheme that compares child entries
             * to the last full lsn doesn't work in recovery land. New child
             * entries may have an earlier lsn than the owning BIN's last full,
             * because of the act of splicing in LNs during recovery.
             *
             * For example, suppose that during LN redo, bin 10 was split into
             * bin 10 and bin 12. That splitting causes a full log.  Then later
             * on, the redo splices LN x, which is from before the last full of
             * bin 10, into bin 10. If we checkpoint allowing deltas after
             * recovery finishes, we won't pick up the LNx diff, because that
             * LN is an earlier lsn than the split-induced full log entry of
             * bin 10.
             */
            if (!readOnly) {
            	CheckpointConfig config = new CheckpointConfig();
            	config.setForce(true);
                env.getCheckpointer().doCheckpoint
                    (config,
                     false, // allowDeltas
                     false, // flushAll
                     false, // deleteAllCleanedFiles
                     "recovery");
            }

        } catch (IOException e) {
            Tracer.trace(env, "RecoveryManager", "recover",
                         "Couldn't recover", e);
            throw new RecoveryException(env, "Couldn't recover: " +
                                        e.getMessage(), e);
        } finally {
            Tracer.trace(Level.INFO, env, "Recovery finished: " + info);
        }

        return info;
    }

    /**
     * Find the end of the log, initialize the FileManager. While we're
     * perusing the log, return the last checkpoint lsn if we happen to see it.
     */
    private DbLsn findEndOfLog(boolean readOnly)
        throws IOException, DatabaseException {

        LastFileReader reader = new LastFileReader(env, readBufferSize);
        reader.setTargetType(LogEntryType.LOG_CKPT_END);

        /* 
         * Tell the reader to iterate through the log file until we hit the end
         * of the log or an invalid entry.
         */
        while (reader.readNextEntry()) {
        }

        /* Now truncate if necessary. */
        if (!readOnly) {
            reader.setEndOfFile();
        }

        /* Tell the fileManager where the end of the log is. */
        info.lastUsedLsn = reader.getLastValidLsn();
        info.nextAvailableLsn = reader.getEndOfLog();
        info.nRepeatIteratorReads += reader.getNRepeatIteratorReads();
        env.getFileManager().setLastPosition(info.nextAvailableLsn,
                                             info.lastUsedLsn,
                                             reader.getPrevOffset());

        /*
         * Now the logging system is initialized and can do more
         * logging. Enable publishing of debug log messages to the database
         * log.
         */
        env.enableDebugLoggingToDbLog();

        return reader.getLastSeen(LogEntryType.LOG_CKPT_END);
    }

    /**
     * Find the last checkpoint and establish the firstActiveLsn point,
     * checkpoint start, and checkpoint end.
     */
    private void findLastCheckpoint(DbLsn checkpointLsn) 
        throws IOException, DatabaseException {

        /* 
         * The checkpointLsn might have been already found when establishing
         * the end of the log. If not, search backwards for it now.
         */
        info.checkpointEndLsn = checkpointLsn;
        if (info.checkpointEndLsn == null) {
            
            /*
             * Search backwards though the log for a checkpoint end entry and a
             * root entry.
             */
            CheckpointFileReader searcher =
                new CheckpointFileReader(env, readBufferSize, false,
                                         info.lastUsedLsn, null,
                                         info.nextAvailableLsn);

            while (searcher.readNextEntry()) {

                /* 
                 * Continue iterating until we find a checkpoint end entry.
                 * While we're at it, remember the last root seen in case we
                 * don't find a checkpoint end entry.
                 */
                if (searcher.isCheckpoint()) {

                    /* 
                     * We're done, the checkpoint end will tell us where the
                     * root is.
                     */
                    info.checkpointEndLsn = searcher.getLastLsn();
                    break;
                } else if (searcher.isRoot()) {

                    /* 
                     * Save the last root in the log in case we don't see a
                     * checkpoint.
                     */
                    if (info.useRootLsn == null) {
                        info.useRootLsn = searcher.getLastLsn();
                    }
                }
            }
            info.nRepeatIteratorReads += searcher.getNRepeatIteratorReads();
        }

        /*
         * If we haven't found a checkpoint, we'll have to recover without
         * one. At a minimium, we must have found a root.
         */
        if (info.checkpointEndLsn == null) {
            info.checkpointStartLsn = null;
            info.firstActiveLsn = null;
        } else {
            /* Read in the checkpoint entry. */
            CheckpointEnd checkpointEnd =
                (CheckpointEnd)(env.getLogManager().get
                                (info.checkpointEndLsn));
            info.checkpointEnd = checkpointEnd;
            info.checkpointStartLsn = checkpointEnd.getCheckpointStartLsn();
            info.firstActiveLsn = checkpointEnd.getFirstActiveLsn();
            if (checkpointEnd.getRootLsn() != null) {
                info.useRootLsn = checkpointEnd.getRootLsn();
            }

            /* Init the checkpointer daemon's id sequence. */
            env.getCheckpointer().setCheckpointId(checkpointEnd.getId());
        }
        if (info.useRootLsn == null) {
            throw new RecoveryException(env,
                                        "This environment's log file has no " +
                                        "root. Since the root is the first " +
                                        "entry written into a log at " +
                                        "environment creation, this should " +
                                        "only happen if the initial creation "+
                                        "of the environment was never " +
                                        "checkpointed or synced. Please move "+
                                        "aside the existing log files to " +
                                        "allow the creation of a new "+
                                        "environment");
        }
    }

    /**
     * Use the log to recreate an in memory tree.
     */
    private void buildTree()
        throws IOException, DatabaseException {

        inListClearCounter = 0;

        /* 
         * Pass 1: Read all map database INs, find largest node id before any
         * possiblity of splits, find largest txn Id before any need for a root
         * update (which would use an AutoTxn)
         */
        Tracer.trace(Level.INFO, env, passStartHeader(1) + "read map INs");
	long start = System.currentTimeMillis();
        readINsAndTrackIds(info.checkpointStartLsn);
	long end = System.currentTimeMillis();
        Tracer.trace(Level.INFO, env, passEndHeader(1, start, end) +
		     info.toString());

        /* 
         * Pass 2: Read map BINDeltas
         */
        Tracer.trace(Level.INFO, env, passStartHeader(2) +
                     "read map BINDeltas");
	start = System.currentTimeMillis();
        info.numOtherINs += readINs(info.checkpointStartLsn, 
                                    true,
                                    LogEntryType.LOG_BIN_DELTA,
                                    null,
                                    null);
	end = System.currentTimeMillis();
        Tracer.trace(Level.INFO, env, passEndHeader(2, start, end) +
		     info.toString());

        /*
         * Pass 3: Count all new entries using the UtilizationTracker.  Also
         * populate the fileSummaryLsns map.
         */
        Tracer.trace(Level.INFO, env, passStartHeader(3) + "count entries");
	start = System.currentTimeMillis();
        countNewEntries(info.checkpointStartLsn);
	end = System.currentTimeMillis();
        Tracer.trace(Level.INFO, env, passEndHeader(3, start, end) +
		     info.toString());

        /*
         * Pass 4: Undo all aborted map lns. Also, read and remember all
         * committed transaction ids.
         */
        Tracer.trace(Level.INFO, env, passStartHeader(4) + "undo map LNs");
	start = System.currentTimeMillis();
        Set mapLNSet = new HashSet();
        mapLNSet.add(LogEntryType.LOG_MAPLN_TRANSACTIONAL);
        mapLNSet.add(LogEntryType.LOG_TXN_COMMIT);
        undoLNs(info, mapLNSet);
	end = System.currentTimeMillis();
        Tracer.trace(Level.INFO, env, passEndHeader(4, start, end) +
		     info.toString());

        /* 
         * Pass 5: Replay all mapLNs, mapping tree in place now. Use the set of
         * committed txns found from pass 4.
         */
        Tracer.trace(Level.INFO, env, passStartHeader(5) + "redo map LNs");
	start = System.currentTimeMillis();
        mapLNSet.add(LogEntryType.LOG_MAPLN);
        redoLNs(info.checkpointStartLsn, mapLNSet);
	end = System.currentTimeMillis();
        Tracer.trace(Level.INFO, env, passEndHeader(5, start, end) +
		     info.toString());

        /*
         * Pass 6: Read all other INs.
         */
        Tracer.trace(Level.INFO, env, passStartHeader(6) + "read other INs");
	start = System.currentTimeMillis();
        info.numOtherINs += readINs(info.checkpointStartLsn, 
                                    false,
                                    LogEntryType.LOG_IN,
                                    LogEntryType.LOG_BIN,
                                    LogEntryType.LOG_IN_DELETE_INFO);
	end = System.currentTimeMillis();
        Tracer.trace(Level.INFO, env, passEndHeader(6, start, end) +
		     info.toString());

        /* 
         * Pass 7: Read BIN Deltas 
         */
        Tracer.trace(Level.INFO, env, passStartHeader(7) + "read BINDeltas");
	start = System.currentTimeMillis();
        info.numBinDeltas = readINs(info.checkpointStartLsn, 
                                    false,
                                    LogEntryType.LOG_BIN_DELTA,
                                    null,
                                    null);
	end = System.currentTimeMillis();
        Tracer.trace(Level.INFO, env, passEndHeader(7, start, end) +
		     info.toString());

        /* 
         * Pass 8: Replay DINs and DBINs.
         */
        Tracer.trace(Level.INFO, env, passStartHeader(8) + "read dup INs");
	start = System.currentTimeMillis();
        info.numDuplicateINs += readINs(info.checkpointStartLsn, 
                                        false,
                                        LogEntryType.LOG_DIN,
                                        LogEntryType.LOG_DBIN,
                                        LogEntryType.LOG_IN_DELETE_INFO);
	end = System.currentTimeMillis();
        Tracer.trace(Level.INFO, env, passEndHeader(8, start, end) +
		     info.toString());

        /* 
         * Pass 9: replay dup BINDeltas
         */
        Tracer.trace(Level.INFO, env, passStartHeader(9) +
                     "read dup BINDeltas");
	start = System.currentTimeMillis();
        info.numBinDeltas += readINs(info.checkpointStartLsn, 
                                     false,
                                     LogEntryType.LOG_DUP_BIN_DELTA,
                                     null,
                                     null);
	end = System.currentTimeMillis();
        Tracer.trace(Level.INFO, env, passEndHeader(9, start, end) +
		     info.toString());

        /*
         * Pass 10: Undo aborted LNs. No need to collect committed txn ids
         * again, was done in pass 4.
         */
        Tracer.trace(Level.INFO, env, passStartHeader(10) + "undo LNs");
	start = System.currentTimeMillis();
        Set lnSet = new HashSet();
        lnSet.add(LogEntryType.LOG_LN_TRANSACTIONAL);
        lnSet.add(LogEntryType.LOG_NAMELN_TRANSACTIONAL);
        lnSet.add(LogEntryType.LOG_DEL_DUPLN_TRANSACTIONAL);
        lnSet.add(LogEntryType.LOG_DUPCOUNTLN_TRANSACTIONAL);

        undoLNs(info, lnSet);
	end = System.currentTimeMillis();
        Tracer.trace(Level.INFO, env, passEndHeader(10, start, end) +
		     info.toString());

        /* Pass 11: Replay LNs. Also read non-transactional lns.*/
        Tracer.trace(Level.INFO, env, passStartHeader(11) + "redo LNs");
	start = System.currentTimeMillis();
        lnSet.add(LogEntryType.LOG_LN);
        lnSet.add(LogEntryType.LOG_NAMELN);
        lnSet.add(LogEntryType.LOG_DEL_DUPLN);
        lnSet.add(LogEntryType.LOG_DUPCOUNTLN);
        lnSet.add(LogEntryType.LOG_FILESUMMARYLN);
        redoLNs(info.checkpointStartLsn, lnSet);
	end = System.currentTimeMillis();
        Tracer.trace(Level.INFO, env, passEndHeader(11, start, end) +
		     info.toString());

        /*
         * Free the memory consumed by the commit list right away, before
         * building the IN list.
         */
        committedTxnIds = null;
        fileSummaryLsns = null;

        /* Rebuild the in memory IN list */
        rebuildINList();
    }

    /*
     * Read every internal node and IN DeleteInfo in the mapping tree and place
     * in the in-memory tree.
     */
    private void readINsAndTrackIds(DbLsn rollForwardLsn) 
        throws IOException, DatabaseException {

        INFileReader reader =
            new INFileReader(env,
                             readBufferSize,
                             rollForwardLsn,
                             true,   // track node and db ids
			     false); // map db only
        reader.addTargetType(LogEntryType.LOG_IN);
        reader.addTargetType(LogEntryType.LOG_BIN);
        reader.addTargetType(LogEntryType.LOG_IN_DELETE_INFO);

        try {
            info.numMapINs = 0;
            DbTree dbMapTree = env.getDbMapTree();

            /* Process every IN and INDeleteInfo in the mapping tree. */
            while (reader.readNextEntry()) {
                DatabaseId dbId = reader.getDatabaseId();
                if (dbId.equals(DbTree.ID_DB_ID)) {
                    DatabaseImpl db = dbMapTree.getDb(dbId);
                    replayOneIN(reader, db);
                    info.numMapINs++;
                }
            }

            /* 
             * Update node id and database sequences. Use either the maximum of
             * the ids seen by the reader vs the ids stored in the checkpoint.
             */
            info.useMaxNodeId = reader.getMaxNodeId();
            info.useMaxDbId = reader.getMaxDbId();
            info.useMaxTxnId = reader.getMaxTxnId();
            if (info.checkpointEnd != null) {
                if (info.useMaxNodeId < info.checkpointEnd.getLastNodeId()) {
                    info.useMaxNodeId = info.checkpointEnd.getLastNodeId();
                }
                if (info.useMaxDbId < info.checkpointEnd.getLastDbId()) {
                    info.useMaxDbId = info.checkpointEnd.getLastDbId();
                }
                if (info.useMaxTxnId < info.checkpointEnd.getLastTxnId()) {
                    info.useMaxTxnId = info.checkpointEnd.getLastTxnId();
                }
            }

            Node.setLastNodeId(info.useMaxNodeId);
            env.getDbMapTree().setLastDbId(info.useMaxDbId);
            env.getTxnManager().setLastTxnId(info.useMaxTxnId);

            info.nRepeatIteratorReads += reader.getNRepeatIteratorReads();
        } catch (Exception e) {
            traceAndThrowException(reader.getLastLsn(), "readMapIns", e);
        }
    }

    /**
     * Read INs and process.
     */
    private int readINs(DbLsn rollForwardLsn,
                        boolean mapDbOnly,
                        LogEntryType inType1,
                        LogEntryType inType2,
                        LogEntryType inType3)
        throws IOException, DatabaseException {

        // don't need to track NodeIds
        INFileReader reader =
	    new INFileReader(env, readBufferSize, rollForwardLsn,
			     false, mapDbOnly);
        if (inType1 != null) {
            reader.addTargetType(inType1);
        }
        if (inType2 != null) {
            reader.addTargetType(inType2);
        }
        if (inType3 != null) {
            reader.addTargetType(inType3);
        }

        int numINsSeen = 0;
        try {

            /*
             * Read all non-provisional INs, and process if they don't belong
             * to the mapping tree.
             */
            DbTree dbMapTree = env.getDbMapTree();
            while (reader.readNextEntry()) {
                DatabaseId dbId = reader.getDatabaseId();
                boolean isMapDb = dbId.equals(DbTree.ID_DB_ID);
                boolean isTarget = false;

                if (mapDbOnly && isMapDb) {
                    isTarget = true;
                } else if (!mapDbOnly && !isMapDb) {
                    isTarget = true;
                }
                if (isTarget) {
                    DatabaseImpl db = dbMapTree.getDb(dbId);
                    if (db == null) {
                        // This db has been deleted, ignore the entry.
                    } else {
                        replayOneIN(reader, db);
                        numINsSeen++;

                        /*
                         * Add any db that we encounter IN's for because
                         * they'll be part of the in-memory tree and therefore
                         * should be included in the INList rebuild.
                         */
                        inListRebuildDbIds.add(dbId);
                    }
                }
            }

            info.nRepeatIteratorReads += reader.getNRepeatIteratorReads();
            return numINsSeen;
        } catch (Exception e) {
            traceAndThrowException(reader.getLastLsn(), "readNonMapIns", e);
            return 0;
        }
    }

    /**
     * Get an IN from the reader, set its database, and fit into tree.
     */
    private void replayOneIN(INFileReader reader, DatabaseImpl db)
        throws DatabaseException {
        
        if (reader.isDeleteInfo()) {
            /* last entry is a delete, replay it. */
            replayINDelete(db,
                           reader.getDeletedNodeId(),
                           reader.getDeletedIdKey(),
                           reader.getLastLsn());
        } else {

            /* 
	     * Last entry is a node, replay it. Now, we should really call
	     * IN.postFetchInit, but we want to do something different from the
	     * faulting-in-a-node path, because we don't want to put the IN on
	     * the in memory list, and we don't want to search the db map tree,
	     * so we have a IN.postRecoveryInit.  Note also that we have to
	     * pass the lsn of the current log entry and also the lsn of the IN
	     * in question. The only time these differ is when the log entry is
	     * a BINDelta -- then the IN's lsn is the last full version lsn,
	     * and the log lsn is the current log entry.
             */
            IN in = reader.getIN();
            DbLsn inLsn = reader.getLsnOfIN();
            in.postRecoveryInit(db, inLsn);
            in.latch();
            replaceOrInsert(db, in, reader.getLastLsn(), inLsn);
        }

        /* 
	 * Although we're careful to not place INs instantiated from the log on
	 * the IN list, we do call normal tree search methods when checking
	 * agains the active tree. The INList builds up from the faulting in of
	 * nodes this way. However, some of those nodes become obsolete as we
	 * splice in newer versions, so the INList becomes too large and can
	 * pose a problem by causing us to overflow memory bounds.  Some
	 * possible solutions are to create a environment wide recovery mode,
	 * or to put special logic into the normal faulting-in path to know
	 * that we're in recovery. Because we don't want to impact normal code
	 * paths, we're going to just periodically clear the INList here. The
	 * INList will be regenerated at the end of recovery.
         */
        if ((inListClearCounter % CLEAR_INCREMENT) == 0) {
            env.getInMemoryINs().clear();        
        } else {
            inListClearCounter++;
        }
    }

    /**
     * Undo all aborted LNs. To do so, walk the log backwards, keeping a
     * collection of committed txns. If we see a log entry that doesn't have a
     * committed txn, undo it.
     */
    private void undoLNs(RecoveryInfo info, Set lnTypes)
        throws IOException, DatabaseException {

	DbLsn firstActiveLsn = info.firstActiveLsn;
	DbLsn lastUsedLsn = info.lastUsedLsn;
	DbLsn endOfFileLsn = info.nextAvailableLsn;
        /* Set up a reader to pick up target log entries from the log. */ 
        LNFileReader reader =
            new LNFileReader(env, readBufferSize, lastUsedLsn, 
                             false, endOfFileLsn, firstActiveLsn, null);

        Iterator iter = lnTypes.iterator();
        while (iter.hasNext()) {
            LogEntryType lnType = (LogEntryType) iter.next();
            reader.addTargetType(lnType);
        }

        Map countedFileSummaries = new HashMap(); // TxnNodeId -> file number
        Set countedAbortLsnNodes = new HashSet(); // set of TxnNodeId

        DbTree dbMapTree = env.getDbMapTree();
        TreeLocation location = new TreeLocation();
        try {

            /*
             * Iterate over the target LNs and commit records, constructing
             * tree.
             */
            while (reader.readNextEntry()) {
                if (reader.isLN()) {

                    /* Get the txnId from the log entry. */
                    Long txnId = reader.getTxnId();

                    /*
                     * If this node is not in a committed txn and it hasn't
                     * been undone before, undo it now.
                     */
                    if (!committedTxnIds.contains(txnId)) {
			LN ln = reader.getLN();
                        DbLsn logLsn = reader.getLastLsn();
                        DbLsn abortLsn = reader.getAbortLsn();
                        boolean abortKnownDeleted =
                            reader.getAbortKnownDeleted();
			DatabaseId dbId = reader.getDatabaseId();
			DatabaseImpl db = dbMapTree.getDb(dbId);
                        
			/* Database may be null if it's been deleted. */
			if (db != null) {
			    ln.postFetchInit(db);
			    try {
				undo(detailedTraceLevel,
				     db,
				     location,
				     ln,
				     reader.getKey(),
				     reader.getDupTreeKey(),
				     logLsn, 
				     abortLsn,
				     abortKnownDeleted,
				     info, true);

				/*
				 * Add any db that we encounter LN's for
				 * because they'll be part of the in-memory
				 * tree and therefore should be included in the
				 * INList rebuild.
				 */
				inListRebuildDbIds.add(dbId);
			    } finally {
				if (location.bin != null) {
				    location.bin.releaseLatch();
				}
			    }
			}
                        /* Undo utilization info. */
                        TxnNodeId txnNodeId =
                            new TxnNodeId(reader.getNodeId(),
                                          txnId.longValue());
                        undoUtilizationInfo(ln, logLsn, abortLsn,
                                            abortKnownDeleted,
                                            txnNodeId,
                                            countedFileSummaries,
                                            countedAbortLsnNodes);
                    }
                } else {
                    /* The entry just read is a commit record. */
                    committedTxnIds.add(new Long(reader.getTxnCommitId()));
                }
            }
            info.nRepeatIteratorReads += reader.getNRepeatIteratorReads();
        } catch (Exception e) {
            traceAndThrowException(reader.getLastLsn(), "undoLNs", e);
        }
    }

    /**
     * Apply all committed LNs
     * @param rollForwardLsn start redoing from this point
     * @param lnType1 targetted LN
     * @param lnType2 targetted LN
     */
    private void redoLNs(DbLsn rollForwardLsn, Set lnTypes)
        throws IOException, DatabaseException {

        /* Set up a reader to pick up target log entries from the log */ 
        LNFileReader reader =
            new LNFileReader(env, readBufferSize, rollForwardLsn,
                             true, null, null, null);

        Iterator iter = lnTypes.iterator();
        while (iter.hasNext()) {
            LogEntryType lnType = (LogEntryType) iter.next();
            reader.addTargetType(lnType);
        }

        Set countedAbortLsnNodes = new HashSet(); // set of TxnNodeId

        DbTree dbMapTree = env.getDbMapTree();
        TreeLocation location = new TreeLocation();
        try {

            /* Iterate over the target LNs and construct in- memory tree. */
            while (reader.readNextEntry()) {
                if (reader.isLN()) {

                    /* Get the txnId from the log entry. */
                    Long txnId = reader.getTxnId();
                
                    /* 
                     * If this LN is in a committed txn, or if it's a
                     * non-transactioal LN, redo it.
                     */
                    if ((txnId == null) ||
                        ((txnId != null) && committedTxnIds.contains(txnId))) {

                        LN ln = reader.getLN();
                        DatabaseId dbId = reader.getDatabaseId();
                        DatabaseImpl db = dbMapTree.getDb(dbId);
                        DbLsn logLsn = reader.getLastLsn();
                        DbLsn treeLsn = null;

                        /* Database may be null if it's been deleted */
                        if (db != null) {
                            ln.postFetchInit(db);
                            treeLsn = redo(db,
                                           location,
                                           ln,
                                           reader.getKey(),
                                           reader.getDupTreeKey(),
                                           logLsn,
                                           info);

                            /*
                             * Add any db that we encounter LN's for because
                             * they'll be part of the in-memory tree and
                             * therefore should be included in the INList
                             * rebuild.
                             */
                            inListRebuildDbIds.add(dbId);
                        }

                        /* Redo utilization info. */
                        TxnNodeId txnNodeId = null;
                        if (txnId != null) {
                            txnNodeId = new TxnNodeId(reader.getNodeId(),
                                                      txnId.longValue());
                        }
                        redoUtilizationInfo(logLsn, treeLsn,
                                            reader.getAbortLsn(),
                                            reader.getAbortKnownDeleted(),
                                            ln, txnNodeId,
                                            countedAbortLsnNodes);
                    }
                }
            }
            info.nRepeatIteratorReads += reader.getNRepeatIteratorReads();
        } catch (Exception e) {
            traceAndThrowException(reader.getLastLsn(), "redoLns", e);
        }
    }

    /**
     * Count all new log entries using the UtilizationTracker and populate the
     * fileSummaryLsns map.
     */
    private void countNewEntries(DbLsn startLsn)
        throws IOException, DatabaseException {

        UtilizationTracker tracker = env.getUtilizationTracker();

        UtilizationFileReader reader =
            new UtilizationFileReader(env, readBufferSize, startLsn,
                                      null, null);
        try {

            /*
             * UtilizationFileReader calls UtilizationTracker.countNewLogEntry
             * for all entries, but only returns FileSummaryLN entries.
             */
            while (reader.readNextEntry()) {

                /*
                 * When a FileSummaryLN is encountered, reset the tracked
                 * summary for that file to replay what happens when a
                 * FileSummaryLN log entry is written.
                 */
                long fileNum = reader.getFileNumber();
                TrackedFileSummary trackedLN = tracker.getTrackedFile(fileNum);
                if (trackedLN != null) {
                    trackedLN.reset();
                }

                /* Save the Lsn of the FileSummaryLN for use by undo/redo. */
                fileSummaryLsns.put(new Long(fileNum),
                                    reader.getLastLsn());

                /*
                 * SR 10395: Do not cache the file summary in the
                 * UtilizationProfile here, since it may be for a deleted log
                 * file.
                 */
            }
            info.nRepeatIteratorReads += reader.getNRepeatIteratorReads();
        } catch (Exception e) {
            traceAndThrowException(reader.getLastLsn(), "countNewEntries", e);
        }
    }

    /**
     * Rebuild the in memory inList with INs that have been made resident by
     * the recovery process.
     */
    private void rebuildINList() 
        throws DatabaseException {

        env.getInMemoryINs().clear();               // empty out
        env.getDbMapTree().rebuildINListMapDb();    // scan map db

        /* For all the dbs that we read in recovery, scan for resident INs. */
        Iterator iter = inListRebuildDbIds.iterator();
        while (iter.hasNext()) {
            DatabaseId dbId = (DatabaseId) iter.next();
            /* We already did the map tree, don't do it again. */
            if (!dbId.equals(DbTree.ID_DB_ID)) {
                DatabaseImpl db = env.getDbMapTree().getDb(dbId);
                db.getTree().rebuildINList();
            }
        }
    }

    /* Struct to hold a nodeId/txnId tuple */
    private static class TxnNodeId {
        long nodeId;
        long txnId;
        
        TxnNodeId(long nodeId, long txnId) {
            this.nodeId = nodeId;
            this.txnId = txnId;
        }

        /**
         * Compare two TxnNodeId objects
         */
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }

            if (!(obj instanceof TxnNodeId)) {
                return false;
            }

            return ((((TxnNodeId) obj).txnId == txnId) &&
                    (((TxnNodeId) obj).nodeId == nodeId));
        }

        public int hashCode() {
            return (int) (txnId + nodeId);
        }

        public String toString() {
            return "txnId=" + txnId + "/nodeId=" + nodeId;
        }
    }

    /*
     * Tree manipulation methods.
     */

    /**
     * Recover an internal node. If inFromLog is:
     *       - not found, insert it in the appropriate location.
     *       - if found and there is a physical match (lsns are the same)
     *         do nothing.
     *       - if found and there is a logical match (lsns are different,
     *         another version of this IN is in place, replace the found node
     *         with the node read from the log only if the log version's
     *         lsn is greater.
     * InFromLog should be latched upon entering this method and it will
     * not be latched upon exiting.
     *
     * @param inFromLog - the new node to put in the tree.  The identifier key
     * and node id are used to find the existing version of the node.
     * @param logLsn - the location of this in in the log.
     */
    private void replaceOrInsert(DatabaseImpl db, IN inFromLog,
                                 DbLsn logLsn, DbLsn inLsn)
        throws DatabaseException {

        List trackingList = null;
        try {

            /*
             * We must know a priori if this node is the root. We can't infer
             * that status from a search of the existing tree, because
             * splitting the root is done by putting a node above the old root.
             * A search downward would incorrectly place the new root below the
             * existing tree.
             */
            if (inFromLog.isRoot()) {
                if (inFromLog.containsDuplicates()) {
                    replaceOrInsertDuplicateRoot(db, (DIN) inFromLog, logLsn);
                } else {
                    replaceOrInsertRoot(db, inFromLog, logLsn);
                }
            } else {
                /*
                 * Look for a parent. The call to getParentNode unlatches node.
                 * Then place inFromLog in the tree if appropriate.
                 */
                trackingList = new ArrayList();
                replaceOrInsertChild(db, inFromLog, logLsn, inLsn,
                                     trackingList);
            }
        } catch (Exception e) {
            String trace = printTrackList(trackingList);
            Tracer.trace(db.getDbEnvironment(), "RecoveryManager",
                         "replaceOrInsert", " lsnFromLog:" +
                         logLsn.getNoFormatString() + " " + trace,
                         e);
            throw new DatabaseException("lsnFromLog=" +
                                        logLsn.getNoFormatString(), e);
        } finally {
            if (inFromLog.getLatch().isOwner()) {
                inFromLog.releaseLatch();
            }

            assert (Latch.countLatchesHeld() == 0):
                Latch.latchesHeldToString() +
                "Lsn = " + logLsn +
                " inFromLog = " + inFromLog.getNodeId();

        }
    }

    /**
     * Dump a tracking list into a string.
     */
    private String printTrackList(List trackingList) {
        if (trackingList != null) {
            StringBuffer sb = new StringBuffer();
            Iterator iter = trackingList.iterator();
            sb.append("Trace list:");
            sb.append('\n');
            while (iter.hasNext()) {
                sb.append((TrackingInfo) iter.next());
                sb.append('\n');
            }
            return sb.toString();
        } else {
            return null;
        }
    }


    /**
     * Replay an IN delete. Remove an entry from an IN to reflect a reverse
     * split.
     *
     * @param logNid node id from the log of IN that had a delete.
     * @param logIdKey which key was deleted.
     */
    private void replayINDelete(DatabaseImpl db, long logNid, Key logIdKey,
                                DbLsn logLsn)
        throws DatabaseException {

        IN parent = null;
        boolean found = false;
        boolean deleted = false;
        int index = -1;

        try {
            Tree tree = db.getTree();
            /* Search for the parent of this target node. */
            parent = tree.search(logIdKey, Tree.SearchType.NORMAL,
                                 logNid);

            /* It's null -- we actually deleted the root. */
            if (parent == null) {
                tree.withRootLatched(new RootDeleter(tree));
                DbTree dbTree = db.getDbEnvironment().getDbMapTree();
                dbTree.modifyDbRoot(db);
                Tracer.traceRootDeletion(Level.FINE, db);
                deleted = true;
            } else {
                index = parent.findEntry(logIdKey, false, true);
                if (index >= 0) {
                    found = true;
                    deleted = parent.deleteEntry(index, false);
                }
            }
        } finally {
            if (parent != null) {
                parent.releaseLatch();
            }

            traceINDeleteReplay(logNid, logLsn, found, deleted, index);
        }
    }

    /*
     * RootDeleter lets us clear the rootIN within the root latch.
     */
    private static class RootDeleter implements WithRootLatched {
        Tree tree;
        RootDeleter(Tree tree) {
            this.tree = tree;
        }

        /**
         * @return true if the in-memory root was replaced.
         */
        public IN doWork(ChildReference root) 
            throws DatabaseException {
            tree.setRoot(null);
            return null;
        }
    }

    /**
     * If the root of this tree is null, use this IN from the log as a root.
     * Note that we should really also check the lsn of the mapLN, because
     * perhaps the root is null because it's been deleted. However, the replay
     * of all the LNs will end up adjusting the tree correctly.
     *
     * If there is a root, check if this IN is a different lsn and if so,
     * replace it.
     */
    private void replaceOrInsertRoot(DatabaseImpl db, IN inFromLog, DbLsn lsn)
        throws DatabaseException {

        boolean success = true;
        Tree tree = db.getTree();
        RootUpdater rootUpdater = new RootUpdater(tree, inFromLog, lsn);
        try {
            /* Run the root updater while the root latch is held. */
            tree.withRootLatched(rootUpdater);

            /* Update the mapLN if necessary */
            if (rootUpdater.updateDone()) {
                EnvironmentImpl env = db.getDbEnvironment();
                env.getDbMapTree().modifyDbRoot(db);
            } 

        } catch (Exception e) {
            success = false;
            throw new DatabaseException("lsnFromLog=" +
                                        lsn.getNoFormatString(),
                                        e);
        } finally {
            trace(detailedTraceLevel,
                  db, TRACE_ROOT_REPLACE, success, inFromLog,
                  lsn, 
                  null,
                  true,
                  rootUpdater.getReplaced(),
                  rootUpdater.getInserted(),
                  rootUpdater.getOriginalLsn(), null, -1);
        }
    }

    /*
     * RootUpdater lets us replace the tree root within the tree root latch.
     */
    private static class RootUpdater implements WithRootLatched {
        private Tree tree;
        private IN inFromLog;
        private DbLsn lsn;
        private boolean inserted = false;
        private boolean replaced = false;
        private DbLsn originalLsn;

        RootUpdater(Tree tree, IN inFromLog, DbLsn lsn) {
            this.tree = tree;
            this.inFromLog = inFromLog;
            this.lsn = lsn;
        }

        /**
         * @return true if the in-memory root was replaced.
         */
        public IN doWork(ChildReference root) 
            throws DatabaseException {

            ChildReference newRoot =
                new ChildReference(inFromLog, new Key(new byte[0]), lsn);
            inFromLog.releaseLatch();

            if (root == null) {
                tree.setRoot(newRoot);
                inserted = true;
            } else {
                originalLsn = root.getLsn(); // for debugLog

                /* 
                 * The current in-memory root IN is older than the root IN from
                 * the log.
                 */
                if (root.getLsn().compareTo(lsn) < 0) {
                    tree.setRoot(newRoot);
                    replaced = true;
                }
            }
            return null;
        }

        boolean updateDone() {
            return inserted || replaced;
        }

        boolean getInserted() {
            return inserted;
        }

        boolean getReplaced() {
            return replaced;
        }

        DbLsn getOriginalLsn() {
            return originalLsn;
        }
    }

    /**
     * Recover this root of a duplicate tree.
     */
    private void replaceOrInsertDuplicateRoot(DatabaseImpl db,
                                              DIN inFromLog,
                                              DbLsn lsn)
        throws DatabaseException {

        boolean found = true;
        boolean inserted = false;
        boolean replaced = false;
        DbLsn originalLsn = null;

        Key mainTreeKey = inFromLog.getMainTreeKey();
        IN parent = null;
        int index = -1;
        boolean success = false;
        try {
            parent = db.getTree().search(mainTreeKey, Tree.SearchType.NORMAL,
                                         -1);
            assert parent instanceof BIN;

            index = parent.findEntry(mainTreeKey, false, true);

            if (index >= 0) {
                /* 
                 * Replace whatever's at this entry, whether it's a LN or an
                 * earlier root DIN as long as the LSN is earlier than the one
                 * we've just read from the log.
                 */
                ChildReference childRef = parent.getEntry(index);
                originalLsn = childRef.getLsn();
                if (originalLsn.compareTo(lsn) < 0) {
                    ChildReference newDupRoot =
                        new ChildReference(inFromLog, mainTreeKey, lsn);
                    parent.setEntry(index, newDupRoot);
                    replaced = true;
                }
            } else {
                ChildReference newRef =
                    new ChildReference(inFromLog, mainTreeKey, lsn);
                boolean insertOk = parent.insertEntry(newRef);
                assert insertOk;
                found = false;
            }
            success = true;
        } finally {
            if (parent != null) {
                parent.releaseLatch();
            }
            trace(detailedTraceLevel,
                  db,
                  TRACE_DUP_ROOT_REPLACE, success, inFromLog,
                  lsn, parent, found,
                  replaced, inserted, originalLsn, null, index);
        }
    }

    /**
     * Decide whether to insert this IN from the log, or replace an existing
     * one.
     * @param db owning database
     * @param inFromLog IN which was instantiated from the log.
     * @param logLsn lsn of this entry
     * @param inLsn lsn of this in -- may not be the same as the log lsn if
     * the current entry is a BINDelta
     * @param trackingList debugging aid to save the path traversed by this
     * recovery call
     */
    private void replaceOrInsertChild(DatabaseImpl db,
                                      IN inFromLog,
                                      DbLsn logLsn,
                                      DbLsn inLsn,
                                      List trackingList)
        throws DatabaseException {

        boolean inserted = false;
        boolean replaced = false;
        DbLsn originalLsn = null;
        boolean success = false;
        SearchResult result = new SearchResult();
        try {
            result = db.getTree().getParentINForChildIN(inFromLog,
							false,
							trackingList);

            /*
             * Does inFromLog exist in this parent?
             *
             * 1. No possible parent -- skip this child. It's represented
             *    by a parent that's later in the log.
             * 2. No match, but a possible parent: insert inFromLog. This
             *    is the case when a tree is bootstrapped, and the root is
             *    written w/0 children.
             * 3. physical match: (lsns same) this lsn is already in place, 
             *                    do nothing.
             * 4. logical match: another version of this IN is in place.
             *                   Replace child with inFromLog if inFromLog's
             *                   lsn is greater.
             */
            if (result.parent == null) {
                return;  // case 1, no possible parent.
            }
            
            Key idKey = result.parent.getChildKey(inFromLog);

            /* Get the key that will locate inFromLog in this parent. */
            if (result.index >= 0) {
                ChildReference childRef = result.parent.getEntry(result.index);
                if (childRef.getLsn().equals(logLsn)) {
                    /* case 3: do nothing */

                } else {
                    /* 
                     * Not an exact physical match, now need to look at child.
                     */
                    if (result.exactParentFound) {
                        originalLsn = childRef.getLsn();
                        
                        /* case 4: It's a logical match, replace */
                        if (originalLsn.compareTo(logLsn) < 0) {
                            /* 
			     * It's a logical match, replace. Put the child
			     * node reference into the parent, as well as the
			     * true lsn of the IN. (If this entry is a
			     * BINDelta, the node has been updated with all the
			     * deltas, but the lsn we want to put in should be
			     * the last full lsn, not the lsn of the BINDelta)
                             */
                            result.parent.updateEntry(result.index,
                                                      inFromLog,
                                                      inLsn);
                            replaced = true;
                        }             
                    } else {
                        /* case 1: no match, insert the new node. */
                        ChildReference ref =
                            new ChildReference(inFromLog, idKey, inLsn);
                        boolean insertOk = result.parent.insertEntry(ref);
                        assert insertOk:
                            "Nomatch, couln't insert for lsn "+ logLsn +
                            " parent=" +  result.parent.getNodeId() +
                            " index=" + result.index; 
                        inserted = true;
                    }
                }
            } else {
                /* case 2: no match */
                ChildReference newRef =
                    new ChildReference(inFromLog, idKey, inLsn);
                boolean insertOk = result.parent.insertEntry(newRef);
                assert insertOk;
                inserted = true;
            }
            success = true;;
        } finally {
            if (result.parent != null) {
                result.parent.releaseLatch();
            }
            trace(detailedTraceLevel, db, 
                  TRACE_IN_REPLACE, success, inFromLog, 
                  logLsn, result.parent,
                  result.exactParentFound, replaced, inserted, 
                  originalLsn, null, result.index);
        }
    }

    /**
     * Redo a committed LN for recovery. 
     *
     * <pre>
     * log LN found  | logLSN > lsn | LN is deleted | action
     *   in tree     | in tree      |               |
     * --------------+--------------+---------------+------------------------
     *     Y         |    N         |    n/a        | no action
     * --------------+--------------+---------------+------------------------
     *     Y         |    Y         |     N         | replace w/log lsn
     * --------------+--------------+---------------+------------------------
     *     Y         |    Y         |     Y         | replace w/log lsn, put
     *               |              |               | on compressor queue
     * --------------+--------------+---------------+------------------------
     *     N         |    n/a       |     N         | insert into tree
     * --------------+--------------+---------------+------------------------
     *     N         |    n/a       |     Y         | no action
     * --------------+--------------+---------------+------------------------
     *
     * </pre>
     *
     * @param location holds state about the search in the tree. Passed
     *  in from the recovery manager to reduce objection creation overhead.
     * @param lnFromLog - the new node to put in the tree. 
     * @param mainKey is the key that navigates us through the main tree
     * @param dupTreeKey is the key that navigates us through the duplicate
     * tree 
     * @param logLsn is the lsn from the just-read log entry
     * @param info is a recovery stats object.
     * @return the lsn found in the tree, or null if not found.
     */
    private DbLsn redo(DatabaseImpl db,
                       TreeLocation location,
                       LN lnFromLog,
                       Key mainKey,
                       Key dupKey,
                       DbLsn logLsn,
                       RecoveryInfo info)
        throws DatabaseException {

        boolean found = false;
        boolean replaced = false;
        boolean inserted = false;
        boolean success = false;
        try {

            /*
             *  Find the BIN which is the parent of this LN.
             */
            location.reset();
            found =
		db.getTree().getParentBINForChildLN
		(location, mainKey, dupKey, lnFromLog, true, false, true);

            if (!found && (location.bin == null)) {

                /* 
                 * There is no possible parent for this LN. This tree was
                 * probably compressed away.
                 */
                success = true;
                return null;
            }

            /*
             * Now we're at the parent for this LN, whether BIN, DBIN or DIN
             */
            if (lnFromLog.containsDuplicates()) {
                if (!found) {
                    throw new DatabaseException
                        ("Couldn't find BIN parent while redo'ing " +
                         "DupCountLN, logLsn=" + logLsn.getNoFormatString());
                }
                DIN duplicateRoot = (DIN)
                    location.bin.getEntry(location.index).
		    fetchTarget(db, location.bin);
                if (logLsn.compareTo(location.childLsn) >= 0) {
                    /* DupCountLN needs replacing. */
                    duplicateRoot.updateDupCountLNRefAndNullTarget(logLsn);
                }
            } else {
                if (found) {

                    /* 
                     * This LN is in the tree. See if it needs replacing.
                     */
                    info.lnFound++;

                    if (logLsn.compareTo(location.childLsn) > 0) { 
                        info.lnReplaced++;
                        replaced = true;

                        /* 
			 * Be sure to make the target null. We don't want this
			 * new LN resident, it will make recovery start
			 * dragging in the whole tree and will consume too much
			 * memory.
                         */
                        location.bin.updateEntry(location.index,
                                                 null,
                                                 logLsn);
                    }

                    /* 
                     * If this entry is deleted, put it on the compressor
                     * queue.
                     */
                    if (logLsn.compareTo(location.childLsn) >= 0) {
                        Key deletedKey =
                            location.bin.containsDuplicates() ?
                            dupKey : mainKey;
                        if (lnFromLog.isDeleted()) {
                            db.getDbEnvironment().addToCompressorQueue
                                (location.bin, deletedKey);
                        }
                    }

                } else {
                    /*
                     * This LN is not in the tree. If it's not deleted, insert
                     * it.
                     */
                    info.lnNotFound++;
                    if (!lnFromLog.isDeleted()) {
                        info.lnInserted++;
                        inserted = true;
                        boolean insertOk = insertRecovery(db,
                                                          location,
                                                          logLsn);
                        assert insertOk;
                    }
                }
            }
            success = true;
            return found ? location.childLsn : null;
        } finally {
            if (location.bin != null) {
                location.bin.releaseLatch();
            }
            trace(detailedTraceLevel, db,
                  TRACE_LN_REDO, success, lnFromLog,
                  logLsn, location.bin, found,
                  replaced, inserted,
                  location.childLsn, null, location.index);
        }
    }

    /**
     * Undo the changes to this node. Here are the rules that govern the action
     * taken.
     *
     * <pre>
     *
     * found LN in  | abortLsn is | logLsn ==       | action taken
     *    tree      | null        | LSN in tree     | by undo
     * -------------+-------------+----------------------------------------
     *      Y       |     N       |      Y          | replace w/abort lsn
     * ------------ +-------------+-----------------+-----------------------
     *      Y       |     Y       |      Y          | remove from tree
     * ------------ +-------------+-----------------+-----------------------
     *      Y       |     N/A     |      N          | no action
     * ------------ +-------------+-----------------+-----------------------
     *      N       |     N/A     |    N/A          | no action (*)
     * (*) If this key is not present in the tree, this record doesn't
     * reflect the IN state of the tree and this log entry is not applicable.
     *
     * </pre>
     * @param location holds state about the search in the tree. Passed
     *  in from the recovery manager to reduce objection creation overhead.
     * @param lnFromLog - the new node to put in the tree. 
     * @param mainKey is the key that navigates us through the main tree
     * @param dupTreeKey is the key that navigates us through the duplicate
     *                   tree 
     * @param logLsn is the lsn from the just-read log entry
     * @param abortLsn gives us the location of the original version of the
     *                 node
     * @param info is a recovery stats object.
     */
    public static boolean undo(Level traceLevel,
                               DatabaseImpl db,
                               TreeLocation location,
                               LN lnFromLog,
                               Key mainKey,
                               Key dupKey,
                               DbLsn logLsn,
                               DbLsn abortLsn,
			       boolean abortKnownDeleted,
                               RecoveryInfo info,
			       boolean splitsAllowed)
        throws DatabaseException {

        boolean found = false;
        boolean replaced = false;
        boolean success = false;

        try {

            /*
             *  Find the BIN which is the parent of this LN.
             */
            location.reset();
            found =
		db.getTree().getParentBINForChildLN(location, mainKey, dupKey,
						    lnFromLog, splitsAllowed,
						    true, false);

            /*
             * Now we're at the rightful parent, whether BIN or DBIN.
             */
            if (lnFromLog.containsDuplicates()) {
                /* This is a dupCountLN. */
                if (!found) {
                    throw new DatabaseException
                        ("Couldn't find duplicateRoot while undo'ing " +
                         "DupCountLN.  duplicateRoot should have been " +
                         "recovered in earlier pass, logLsn=" +
                         logLsn.getNoFormatString());
                }
                DIN duplicateRoot = (DIN)
                    location.bin.getEntry(location.index).
		    fetchTarget(db, location.bin);
                if (logLsn.compareTo(location.childLsn) == 0) {
                    /* DupCountLN needs replacing. */
                    duplicateRoot.updateDupCountLNRefAndNullTarget(abortLsn);
                    replaced = true;
                }
            } else {
                if (found) {
                    /* This LN is in the tree. See if it needs replacing. */
                    if (info != null) {
                        info.lnFound++;
                    }
		    boolean updateEntry =
			logLsn.compareTo(location.childLsn) == 0;
		    if (updateEntry) {
			if (abortLsn == null) {

			    /*
			     * To undo a node that was created by this txn,
			     * remove it.  If this entry is deleted, put it on
			     * the compressor queue.
			     */
			    location.bin.
				setKnownDeletedLeaveTarget(location.index);
                            Key deletedKey =
                                location.bin.containsDuplicates() ?
                                dupKey : mainKey;
			    db.getDbEnvironment().addToCompressorQueue
				(location.bin, deletedKey);
                            
			} else {

			    /*
			     * Apply the log record by updating the in memory
			     * tree slot to contain the abort LSN and abort
			     * Known Deleted flag.
			     */
			    if (info != null) {
				info.lnReplaced++;
			    }
			    replaced = true;
			    location.bin.updateEntry(location.index,
						     null,
						     abortLsn);
			    if (abortKnownDeleted) {
				location.bin.setKnownDeleted(location.index);
			    } else {
				location.bin.clearKnownDeleted(location.index);
			    }
			}
		    }

                } else {

                    /* 
                     * This LN is not in the tree.  Just make a note of it.
                     */
                    if (info != null) {
                        info.lnNotFound++;
                    }
                }
            }

            success = true;
            return replaced;
        } finally {
            /* 
             * Note that undo relies on the caller to unlatch the bin.  Not
             * ideal, done in order to support abort processing.
             */
            trace(traceLevel, db, TRACE_LN_UNDO, success, lnFromLog,
		  logLsn, location.bin, found, replaced, false,
		  location.childLsn, abortLsn, location.index);
        }
    }

    /**
     * Inserts a LN into the tree for recovery or abort processing.  In this
     * case, we know we don't have to lock when checking child LNs for deleted
     * status (there can be no other thread running on this tree) and we don't
     * have to log the new entry. (it's in the log already)
     *
     * @param db
     * @param location this embodies the parent bin, the index, the key that
     * represents this entry in the bin.
     * @param logLsn lsn of this current ln
     * @param key to use when creating a new ChildReference object.
     * @return true if LN was inserted, false if it was a duplicate
     * duplicate or if an attempt was made to insert a duplicate when
     * allowDuplicates was false.
     */
    private static boolean insertRecovery(DatabaseImpl db,
                                          TreeLocation location,
                                          DbLsn logLsn) 
        throws DatabaseException {
        
        /* Make a child reference as a candidate for insertion. */
        ChildReference newLNRef =
	    new ChildReference(null, location.lnKey, logLsn);

        BIN parentBIN = location.bin;
        int entryIndex = parentBIN.insertEntry1(newLNRef);

        if ((entryIndex & IN.INSERT_SUCCESS) == 0) {

            /* 
	     * Entry may have been a duplicate. Insertion was not successful.
	     */
            entryIndex &= ~IN.EXACT_MATCH;

            boolean canOverwrite = false;
            ChildReference currentRef = parentBIN.getEntry(entryIndex);
            if (currentRef.isKnownDeleted()) {
                canOverwrite = true;
            } else {
                /*
                 * Read the LN that's in this slot to check for deleted
                 * status. No need to lock, since this is recovery.
                 */
                LN currentLN = (LN) currentRef.fetchTarget(db, parentBIN);

                if (currentLN.isDeleted()) {
                    canOverwrite = true;
                }

                /* 
		 * Evict the target again manually, to reduce memory
		 * consumption while the evictor is not running.
                 */
                currentRef.clearTarget();
            }

            if (canOverwrite) {
                parentBIN.updateEntry(entryIndex, null, logLsn,
                                      location.lnKey);
                parentBIN.clearKnownDeleted(entryIndex);
                location.index = entryIndex;
                return true;
            } else {
                return false;
            }
        }
        location.index = entryIndex & ~IN.INSERT_SUCCESS;
        return true;
    }

    /**
     * Update file utilization info during redo.
     */
    private void redoUtilizationInfo(DbLsn logLsn, DbLsn treeLsn,
                                     DbLsn abortLsn, boolean abortKnownDeleted,
                                     LN ln, TxnNodeId txnNodeId,
                                     Set countedAbortLsnNodes) {

        UtilizationTracker tracker = env.getUtilizationTracker();

        /*
         * If the LN is marked deleted and its Lsn follows the FileSummaryLN
         * for its file, count it as obsolete.
         */
        if (ln.isDeleted()) {
            Long logFileNum = new Long(logLsn.getFileNumber());
            DbLsn fileSummaryLsn = (DbLsn) fileSummaryLsns.get(logFileNum);
            int cmpFsLsnToLogLsn = (fileSummaryLsn != null) ?
                                    fileSummaryLsn.compareTo(logLsn) : -1;
            if (cmpFsLsnToLogLsn < 0) {
                tracker.countObsoleteNode(logLsn, null, true);
            }
        }

        /* Was the LN found in the tree? */
        if (treeLsn != null) {
            int cmpLogLsnToTreeLsn = logLsn.compareTo(treeLsn);

            /*
             * If the oldLsn and newLsn differ and the newLsn follows the
             * FileSummaryLN for the file of the oldLsn, count the oldLsn as
             * obsolete.
             */
            if (cmpLogLsnToTreeLsn != 0) {
                DbLsn newLsn = (cmpLogLsnToTreeLsn < 0) ? treeLsn : logLsn;
                DbLsn oldLsn = (cmpLogLsnToTreeLsn > 0) ? treeLsn : logLsn;
                Long oldLsnFile = new Long(oldLsn.getFileNumber());
                DbLsn oldFsLsn = (DbLsn) fileSummaryLsns.get(oldLsnFile);
                int cmpOldFsLsnToNewLsn = (oldFsLsn != null) ?
                                           oldFsLsn.compareTo(newLsn) : -1;
                if (cmpOldFsLsnToNewLsn < 0) {
                    tracker.countObsoleteNode(oldLsn, null, true);
                }
            }

            /*
             * If the logLsn is equal to or precedes the treeLsn and the entry
             * has an abortLsn that was not previously deleted, consider the
             * set of entries for the given node.  If the logLsn is the first
             * in the set that follows the FileSummaryLN of the abortLsn, count
             * the abortLsn as obsolete.
             */
            if (cmpLogLsnToTreeLsn <= 0 &&
                abortLsn != null &&
                !abortKnownDeleted &&
                !countedAbortLsnNodes.contains(txnNodeId)) {
                /* We have not counted this abortLsn yet. */
                Long abortFileNum = new Long(abortLsn.getFileNumber());
                DbLsn abortFsLsn = (DbLsn) fileSummaryLsns.get(abortFileNum);
                int cmpAbortFsLsnToLogLsn = (abortFsLsn != null) ?
                                             abortFsLsn.compareTo(logLsn) : -1;
                if (cmpAbortFsLsnToLogLsn < 0) {
                    /* logLsn follows the FileSummaryLN of the abortLsn. */
                    tracker.countObsoleteNode(abortLsn, null, true);
                    /* Don't count this abortLsn (this node) again. */
                    countedAbortLsnNodes.add(txnNodeId);
                }
            }
        }
    }

    /**
     * Update file utilization info during recovery undo (not abort undo).
     */
    private void undoUtilizationInfo(LN ln, DbLsn logLsn, DbLsn abortLsn,
                                     boolean abortKnownDeleted,
                                     TxnNodeId txnNodeId,
                                     Map countedFileSummaries,
                                     Set countedAbortLsnNodes) {

        UtilizationTracker tracker = env.getUtilizationTracker();

        /* Compare the fileSummaryLsn to the logLsn. */
        Long logFileNum = new Long(logLsn.getFileNumber());
        DbLsn fileSummaryLsn = (DbLsn) fileSummaryLsns.get(logFileNum);
        int cmpFsLsnToLogLsn = (fileSummaryLsn != null) ?
                                fileSummaryLsn.compareTo(logLsn) : -1;
        /*
         * Count the logLsn as obsolete if it follows the FileSummaryLN for the
         * file of its Lsn.
         */
        if (cmpFsLsnToLogLsn < 0) {
            tracker.countObsoleteNode(logLsn, null, true);
        }

        /*
         * Consider the latest Lsn for the given node that precedes the
         * FileSummaryLN for the file of its Lsn.  Count this Lsn as obsolete
         * if it is not a deleted LN.
         */
        if (cmpFsLsnToLogLsn > 0) {
            Long countedFile = (Long) countedFileSummaries.get(txnNodeId);
            if (countedFile == null ||
                countedFile.longValue() > logFileNum.longValue()) {

                /*
                 * We encountered a new file number and the FsLsn follows the
                 * logLsn.
                 */
                if (!ln.isDeleted()) {
                    tracker.countObsoleteNode(logLsn, null, true);
                }
                /* Don't count this file again. */
                countedFileSummaries.put(txnNodeId, logFileNum);
            }
        }

        /*
         * Count the abortLsn as non-obsolete (if it was not previously
         * deleted) if any Lsn for the given node precedes the FileSummaryLN
         * for the file of the abortLsn.
         */
        if (abortLsn != null &&
            !abortKnownDeleted &&
            !countedAbortLsnNodes.contains(txnNodeId)) {
            /* We have not counted this abortLsn yet. */
            Long abortFileNum = new Long(abortLsn.getFileNumber());
            DbLsn abortFsLsn = (DbLsn) fileSummaryLsns.get(abortFileNum);
            int cmpAbortFsLsnToLogLsn = (abortFsLsn != null) ?
                                         abortFsLsn.compareTo(logLsn) : -1;
            if (cmpAbortFsLsnToLogLsn > 0) {
                /* logLsn precedes the FileSummaryLN of the abortLsn. */
                tracker.countObsoleteNode(abortLsn, null, false);
                /* Don't count this abortLsn (this node) again. */
                countedAbortLsnNodes.add(txnNodeId);
            }
        }
    }

    /**
     * Concoct a header for the recovery pass trace info.
     */
    private String passStartHeader(int passNum) {
        return "Recovery Pass " + passNum + " start: ";
    }

    /**
     * Concoct a header for the recovery pass trace info.
     */
    private String passEndHeader(int passNum, long start, long end) {
        return "Recovery Pass " + passNum + " end (" +
            (end-start) + "): ";
    }

    /**
     * Send trace messages to the java.util.logger. Don't rely on the logger
     * alone to conditionalize whether we send this message, we don't even want
     * to construct the message if the level is not enabled. This is used to
     * construct verbose trace messages for individual log entry processing.
     */
    private static void trace(Level level,
                              DatabaseImpl database,
                              String debugType,
                              boolean success,
                              Node node,
                              DbLsn logLsn,
                              IN parent,
                              boolean found,
                              boolean replaced,
                              boolean inserted,
                              DbLsn replacedLsn, DbLsn abortLsn, int index) {
        Logger logger = database.getDbEnvironment().getLogger();
        Level useLevel= level;
        if (!success) {
            useLevel = Level.SEVERE;
        }
        if (logger.isLoggable(useLevel)) {
            StringBuffer sb = new StringBuffer();
            sb.append(debugType);
            sb.append(" success=").append(success);
            sb.append(" node=");
            sb.append(node.getNodeId());
            sb.append(" lsn=");
            sb.append(logLsn.getNoFormatString());
            if (parent != null) {
                sb.append(" parent=").append(parent.getNodeId());
            }
            sb.append(" found=");
            sb.append(found);
            sb.append(" replaced=");
            sb.append(replaced);
            sb.append(" inserted=");
            sb.append(inserted);
            if (replacedLsn != null) {
                sb.append(" replacedLsn=");
                sb.append(replacedLsn.getNoFormatString());
            }
            if (abortLsn != null) {
                sb.append(" abortLsn=");
                sb.append(abortLsn.getNoFormatString());
            }
            sb.append(" index=").append(index);
            logger.log(useLevel, sb.toString());
        }
    }

    /**
     * Send trace messages to the java.util.logger. Don't rely on the logger
     * alone to conditionalize whether we send this message, we don't even want
     * to construct the message if the level is not enabled.
     */
    private void traceINDeleteReplay(long nodeId,
                                     DbLsn logLsn,
                                     boolean found, 
                                     boolean deleted,
                                     int index) {
        Logger logger = env.getLogger();
        if (logger.isLoggable(detailedTraceLevel)) {
            StringBuffer sb = new StringBuffer();
            sb.append(TRACE_IN_DEL_REPLAY);
            sb.append(" node=").append(nodeId);
            sb.append(" lsn=").append(logLsn.getNoFormatString());
            sb.append(" found=").append(found);
            sb.append(" deleted=").append(deleted);
            sb.append(" index=").append(index);
            logger.log(detailedTraceLevel, sb.toString());
        }
    }

    private void traceAndThrowException(DbLsn badLsn,
                                       String method,
                                       Exception originalException) 
        throws DatabaseException {
        String badLsnString = badLsn.getNoFormatString();
        Tracer.trace(env,
                     "RecoveryManager",
                     method,
                     "last Lsn = " + badLsnString,
                     originalException);
        throw new DatabaseException("last Lsn=" + badLsnString, 
                                    originalException);
    }
}
