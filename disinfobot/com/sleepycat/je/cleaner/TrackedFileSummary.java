/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: TrackedFileSummary.java,v 1.1 2004/11/22 18:27:58 kate Exp $
*/

package com.sleepycat.je.cleaner;

/**
 * Delta file summary info for a tracked file.  Tracked files are managed by
 * the UtilizationTracker.
 */
public class TrackedFileSummary extends FileSummary {

    private UtilizationTracker tracker;
    private long fileNum;

    /**
     * Creates an empty tracked summary.
     */
    TrackedFileSummary(UtilizationTracker tracker, long fileNum) {
        this.tracker = tracker;
        this.fileNum = fileNum;
    }

    /**
     * Returns the file number being tracked.
     */
    public long getFileNumber() {
        return fileNum;
    }

    /**
     * Overrides reset for a tracked file, and is called when a FileSummaryLN
     * is written to the log.
     * 
     * <p>Must be called under the log write latch.</p>
     */
    public void reset() {
        /*
         * Call resetFile before resetting the totals, so the tracker can
         * update the logSizeDelta.
         */
        tracker.resetFile(this);
        super.reset();
    }
}
