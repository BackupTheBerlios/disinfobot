/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: TrackingInfo.java,v 1.1 2004/11/22 18:27:57 kate Exp $
*/

package com.sleepycat.je.tree;

import com.sleepycat.je.utilint.DbLsn;

/**
 * Tracking info packages some tree tracing info.
 */
public class TrackingInfo {
    private DbLsn lsn;
    private long nodeId;
    
    public TrackingInfo(DbLsn lsn, long nodeId) {
        this.lsn = lsn;
        this.nodeId = nodeId;
    }

    public String toString() {
        return "lsn=" + lsn.getNoFormatString() +
            " node=" + nodeId;
    }
}
