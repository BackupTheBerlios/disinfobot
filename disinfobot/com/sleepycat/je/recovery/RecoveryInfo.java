/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: RecoveryInfo.java,v 1.1 2004/11/22 18:27:59 kate Exp $
*/

package com.sleepycat.je.recovery;

import com.sleepycat.je.utilint.DbLsn;

/**
 * RecoveryInfo keeps information about recovery processing. 
 */
public class RecoveryInfo {
    // Locations found during recovery
    public DbLsn lastUsedLsn;      // location of last entry
    public DbLsn nextAvailableLsn; // EOF, location of first unused spot
    public DbLsn firstActiveLsn;
    public DbLsn checkpointStartLsn;
    public DbLsn checkpointEndLsn;
    public DbLsn useRootLsn;  

    // Checkpoint record used for this recovery.
    public CheckpointEnd checkpointEnd;

    // Ids
    public long useMaxNodeId;
    public int useMaxDbId;
    public long useMaxTxnId;

    // num nodes read
    public int numMapINs;
    public int numOtherINs;
    public int numBinDeltas;
    public int numDuplicateINs;

    // ln processing
    public int lnFound;
    public int lnNotFound;
    public int lnInserted;
    public int lnReplaced;

    // FileReader behavior
    public int nRepeatIteratorReads;

    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("Recovery Info");
        appendLsn(sb, " lastUsed=", lastUsedLsn);
        appendLsn(sb, " nextAvail=", nextAvailableLsn);
        appendLsn(sb, " ckptStart=", checkpointStartLsn);
        appendLsn(sb, " firstActive=", firstActiveLsn);
        appendLsn(sb, " ckptEnd=", checkpointEndLsn);
        appendLsn(sb, " useRoot=", useRootLsn);
        sb.append(" ckpt=").append(checkpointEnd);
        sb.append(" useMaxNodeId=").append(useMaxNodeId);
        sb.append(" useMaxDbId=").append(useMaxDbId);
        sb.append(" useMaxTxnId=").append(useMaxTxnId);
        sb.append(" numMapINs=").append(numMapINs);
        sb.append(" numOtherINs=").append(numOtherINs);
        sb.append(" numBinDeltas=").append(numBinDeltas);
        sb.append(" numDuplicateINs=").append(numDuplicateINs);
        sb.append(" lnFound=").append(lnFound);
        sb.append(" lnNotFound=").append(lnNotFound);
        sb.append(" lnInserted=").append(lnInserted);
        sb.append(" lnReplaced=").append(lnReplaced);
        sb.append(" nRepeatIteratorReads=").append(nRepeatIteratorReads);
        return sb.toString();
    }

    private void appendLsn(StringBuffer sb, String name, DbLsn lsn) {
        if (lsn != null) {
            sb.append(name).append(lsn.getNoFormatString());
        }
    }
}
