/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: LogEntryType.java,v 1.1 2004/11/22 18:27:58 kate Exp $
*/

package com.sleepycat.je.log;

import java.util.Hashtable;
import java.util.Map;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.log.entry.BINDeltaLogEntry;
import com.sleepycat.je.log.entry.DeletedDupLNLogEntry;
import com.sleepycat.je.log.entry.INLogEntry;
import com.sleepycat.je.log.entry.LNLogEntry;
import com.sleepycat.je.log.entry.LogEntry;
import com.sleepycat.je.log.entry.SingleItemLogEntry;

/**
 * LogEntryType is a type safe enumeration of all log entry types. 
 */
public class LogEntryType {

    /* 
     * Collection of log entry type classes, used to read the log.  Note that
     * this must be declared before any instances of LogEntryType, since the
     * constructor uses this map. Each statically defined LogEntryType should
     * register itself with this collection.
     */ 
    private static Map LOG_TYPES = new Hashtable();

    /*
     * Enumeration of log entry types. The log entry value represents the 2
     * byte field that starts every log entry. The top byte is the log type,
     * the bottom byte holds the version value and provisional bit.
     *
     *  Logtype (8 bits) (Provisional (1 bit) Version (7 bits)
     *
     * The provisional bit can be set for any log type in the log. It's an
     * indication to recovery that the entry shouldn't be processed when
     * rebuilding the tree. It's used to ensure the atomic logging of multiple
     * entries.
     */

    /*  Node types */
    public static final LogEntryType LOG_LN_TRANSACTIONAL =
        new LogEntryType((byte) 1, (byte) 0, "LN_TX",
			 new LNLogEntry(com.sleepycat.je.tree.LN.class,
					true));

    public static final LogEntryType LOG_LN =
        new LogEntryType((byte) 2, (byte) 0, "LN",
			 new LNLogEntry(com.sleepycat.je.tree.LN.class,
					false));

    public static final LogEntryType LOG_MAPLN_TRANSACTIONAL =
        new LogEntryType((byte) 3, (byte) 0, "MapLN_TX",
			 new LNLogEntry(com.sleepycat.je.tree.MapLN.class,
					true));

    public static final LogEntryType LOG_MAPLN =
        new LogEntryType((byte) 4, (byte) 0, "MapLN",
			 new LNLogEntry(com.sleepycat.je.tree.MapLN.class,
					false));

    public static final LogEntryType LOG_NAMELN_TRANSACTIONAL =
        new LogEntryType((byte) 5, (byte) 0, "NameLN_TX",
			 new LNLogEntry(com.sleepycat.je.tree.NameLN.class,
					true));

    public static final LogEntryType LOG_NAMELN =
        new LogEntryType((byte) 6, (byte) 0, "NameLN",
			 new LNLogEntry(com.sleepycat.je.tree.NameLN.class,
					false));

    public static final LogEntryType LOG_DEL_DUPLN_TRANSACTIONAL =
        new LogEntryType((byte) 7, (byte) 0, "DelDupLN_TX",
			 new DeletedDupLNLogEntry(true));

    public static final LogEntryType LOG_DEL_DUPLN =
        new LogEntryType((byte) 8, (byte) 0, "DelDupLN",
			 new DeletedDupLNLogEntry(false));

    public static final LogEntryType LOG_DUPCOUNTLN_TRANSACTIONAL =
        new LogEntryType
	((byte) 9, (byte) 0, "DupCountLN_TX",
	 new LNLogEntry(com.sleepycat.je.tree.DupCountLN.class, true));

    public static final LogEntryType LOG_DUPCOUNTLN =
        new LogEntryType
	((byte) 10, (byte) 0, "DupCountLN",
	 new LNLogEntry(com.sleepycat.je.tree.DupCountLN.class, false));

    public static final LogEntryType LOG_FILESUMMARYLN =
        new LogEntryType
	((byte) 11, (byte) 0, "FileSummaryLN",
	 new LNLogEntry(com.sleepycat.je.tree.FileSummaryLN.class, false));

    public static final LogEntryType LOG_IN =
        new LogEntryType
	((byte) 12, (byte) 0, "IN",
	 new INLogEntry(com.sleepycat.je.tree.IN.class));

    public static final LogEntryType LOG_BIN =
        new LogEntryType
	((byte) 13, (byte) 0, "BIN",
	 new INLogEntry(com.sleepycat.je.tree.BIN.class));

    public static final LogEntryType LOG_DIN =
        new LogEntryType
	((byte) 14, (byte) 0, "DIN",
	 new INLogEntry(com.sleepycat.je.tree.DIN.class));

    public static final LogEntryType LOG_DBIN =
        new LogEntryType
	((byte) 15, (byte) 0, "DBIN",
	 new INLogEntry(com.sleepycat.je.tree.DBIN.class));

    private static final int MAX_NODE_TYPE_NUM = 15;

    public static boolean isNodeType(byte typeNum, byte version) {
        return (typeNum <= MAX_NODE_TYPE_NUM);
    }

    /* Root */
    public static final LogEntryType LOG_ROOT =
	new LogEntryType((byte) 16, (byte) 0, "Root",
			 new SingleItemLogEntry
			     (com.sleepycat.je.dbi.DbTree.class));

    /* Transactional entries */
    public static final LogEntryType LOG_TXN_COMMIT =
        new LogEntryType((byte) 17, (byte) 0, "Commit",
			 new SingleItemLogEntry
			     (com.sleepycat.je.txn.TxnCommit.class));

    public static final LogEntryType LOG_TXN_ABORT =
        new LogEntryType((byte) 18, (byte) 0, "Abort", 
			 new SingleItemLogEntry
			     (com.sleepycat.je.txn.TxnAbort.class));

    public static final LogEntryType LOG_CKPT_START =
        new LogEntryType
	((byte) 19, (byte) 0, "CkptStart",
	 new SingleItemLogEntry
	     (com.sleepycat.je.recovery.CheckpointStart.class));

    public static final LogEntryType LOG_CKPT_END =
        new LogEntryType((byte) 20, (byte) 0, "CkptEnd",
			 new SingleItemLogEntry
			     (com.sleepycat.je.recovery.CheckpointEnd.class));

    public static final LogEntryType LOG_IN_DELETE_INFO =
        new LogEntryType((byte) 21, (byte) 0, "INDelete",
			 new SingleItemLogEntry
			     (com.sleepycat.je.tree.INDeleteInfo.class));

    public static final LogEntryType LOG_BIN_DELTA =
        new LogEntryType((byte) 22, (byte) 0, "BINDelta",
			 new BINDeltaLogEntry
			     (com.sleepycat.je.tree.BINDelta.class));
            
    public static final LogEntryType LOG_DUP_BIN_DELTA =
        new LogEntryType((byte) 23, (byte) 0, "DupBINDelta", 
			 new BINDeltaLogEntry
			     (com.sleepycat.je.tree.BINDelta.class));

    /* Administrative entries */
    public static final LogEntryType LOG_TRACE =
        new LogEntryType((byte) 24, (byte) 0, "Trace",
			 new SingleItemLogEntry
			     (com.sleepycat.je.utilint.Tracer.class));

    /* File header */
    public static final LogEntryType LOG_FILE_HEADER =
        new LogEntryType((byte) 25, (byte) 0, "FileHeader",
			 new SingleItemLogEntry
			     (com.sleepycat.je.log.FileHeader.class));

    /* for validity checking */
    private static final int MAX_TYPE_NUM = 25;

    private static final byte PROVISIONAL_MASK = (byte)0x80;
    private static final byte IGNORE_PROVISIONAL = ~PROVISIONAL_MASK;

    /*
     * Implementation of a log entry.
     */
    private byte typeNum; // persistent value for this entry type
    private byte version; // for upgrades
    private String displayName;
    private LogEntry logEntry;

    /*
     * Constructors 
     */

    /** 
     * For base class support.
     */
    /*
      LogEntryType() {
      }
    */

    /* No log types can be defined outside this package. */
    LogEntryType(byte typeNum, byte version) {
        this.typeNum = typeNum;
        this.version = version;
    }

    /**
     * Create the static log types.
     */
    private LogEntryType(byte typeNum,
			 byte version,
			 String displayName,
			 LogEntry logEntry) {

        this.typeNum = typeNum;
        this.version = version;
        this.logEntry = logEntry;
        this.displayName = displayName;
        LOG_TYPES.put(this, this);
    }

    public boolean isNodeType() {
        return (typeNum <= MAX_NODE_TYPE_NUM);
    }

    /**
     * @return the static version of this type
     */
    static LogEntryType findType(byte typeNum, byte version) {
        return (LogEntryType)
	    LOG_TYPES.get(new LogEntryType(typeNum, version));
    }

    /**
     * @return the log entry type owned by the shared, static version
     */
    LogEntry getSharedLogEntry() {
        return logEntry;
    }

    /**
     * @return a clone of the log entry type for a given log type.
     */
    LogEntry getNewLogEntry()
        throws DatabaseException {

        try {
            return (LogEntry) logEntry.clone();
        } catch (CloneNotSupportedException e) {
            throw new DatabaseException(e);
        }
    }

    /**
     * Set the provisional bit.
     */
    static byte setProvisional(byte version) {
        return (byte)(version | PROVISIONAL_MASK);
    }

    /**
     * @return true if the provisional bit is set.
     */
    static boolean isProvisional(byte version) {
        return ((version & PROVISIONAL_MASK) != 0);
    }

    byte getTypeNum() {
        return typeNum;
    }

    byte getVersion() {
        return version;
    }

    /**
     * @return true if type number is valid.
     */
    static boolean isValidType(byte typeNum) {
        return typeNum <= MAX_TYPE_NUM;
    }

    public String toString() {
        return displayName + "/" + version;
    }

    /**
     * Check for equality without making a new object.
     */
    boolean equalsType(byte typeNum, byte version) {
        return ((this.typeNum == typeNum) &&
                ((this.version & IGNORE_PROVISIONAL) ==
                 (version & IGNORE_PROVISIONAL)));
    }

    boolean equalsType(byte typeNum) {
        return (this.typeNum == typeNum);
    }

    /* 
     * Override Object.equals. Ignore provisional bit when checking for
     * equality.
     */
    public boolean equals(Object obj) {
        // Same instance?
        if (this == obj) {
            return true;
        }

        // Is it the right type of object?
        if (!(obj instanceof LogEntryType)) {
            return false;
        }

        return ((typeNum == ((LogEntryType) obj).typeNum) &&
                ((version & IGNORE_PROVISIONAL) ==
                 (((LogEntryType)obj).version & IGNORE_PROVISIONAL)));
    }

    /**
     * This is used as a hash key.
     */
    public int hashCode() {
        return typeNum + (version & IGNORE_PROVISIONAL);
    }
}
