/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: FileSummary.java,v 1.1 2004/11/22 18:27:58 kate Exp $
*/

package com.sleepycat.je.cleaner;

import java.nio.ByteBuffer;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.log.LogUtils;

public class FileSummary {

    private static final int MIN_FILES_FOR_AGING = 10;

    /* Persistent fields. */
    public int totalCount;      // Total # of log entries
    public int totalSize;       // Total bytes in log file
    public int totalINCount;    // Number of IN log entries
    public int totalINSize;     // Byte size of IN log entries
    public int totalLNCount;    // Number of LN log entries
    public int totalLNSize;     // Byte size of LN log entries
    public int obsoleteLNCount; // Number of obsolete LN log entries

    /**
     * Creates an empty summary.
     */
    public FileSummary() {
    }

    /**
     * Returns whether this summary contains any non-zero totals.
     */
    public boolean isEmpty() {
        return totalCount == 0 && totalSize == 0 && obsoleteLNCount == 0;
    }

    /**
     * Returns the approximate byte size of all obsolete LN entries.
     */
    public int getObsoleteLNSize() {

        if (totalLNCount == 0) {
            return 0;
        }
        /* Use long arithmetic. */
        long totalSize = totalLNSize;
        /* Scale by 255 to reduce integer truncation error. */
        totalSize <<= 8;
        long avgSizePerLN = totalSize / totalLNCount;
        return (int) ((obsoleteLNCount * avgSizePerLN) >> 8);
    }

    /**
     * Returns an estimate of the total bytes that are obsolete.
     * @param fileIndex file number from 0 to (nFiles - 1).
     */
    public int getObsoleteSize(int fileIndex, UtilizationProfile profile)
        throws DatabaseException {

        if (totalSize > 0) {

            /* Reverse fileIndex so it indicates age. */
            int nFiles = profile.getNumberOfFiles();
            fileIndex = nFiles - fileIndex;

            /* Index above which all INs are considered obsolete. */
            int obsoleteAgeIndex =
                (int) ((profile.getObsoleteAge() * ((long) nFiles)) / 100);

            int obsoleteINSize;

            if (nFiles < MIN_FILES_FOR_AGING) {
                 /* Do not age if there are a small number of files. */
                obsoleteINSize = 0;
            } else if (fileIndex >= obsoleteAgeIndex) {
                /* INs are considered 100% obsolete because of old age. */
                obsoleteINSize = totalINSize;
            } else {
                /* Scale IN obsolesence according to the age of the file. */
                int ageRatio = (int) ((fileIndex * 100L) / obsoleteAgeIndex);
                obsoleteINSize =
                    (int) ((totalINSize * ((long) ageRatio)) / 100);
            }

            int leftoverSize = totalSize - (totalINSize + totalLNSize);

            return getObsoleteLNSize() + obsoleteINSize + leftoverSize;
        } else {
            return 0;
        }
    }

    /**
     * Reset all totals to zero.
     */
    public void reset() {

        totalCount = 0;
        totalSize = 0;
        totalINCount = 0;
        totalINSize = 0;
        totalLNCount = 0;
        totalLNSize = 0;
        obsoleteLNCount = 0;
    }

    /**
     * Add the totals of the given summary object to the totals of this object.
     */
    public void add(FileSummary o) {

        totalCount += o.totalCount;
        totalSize += o.totalSize;
        totalINCount += o.totalINCount;
        totalINSize += o.totalINSize;
        totalLNCount += o.totalLNCount;
        totalLNSize += o.totalLNSize;
        obsoleteLNCount += o.obsoleteLNCount;
    }

    public int getLogSize() {

        return 8 * LogUtils.getIntLogSize();
    }

    public void writeToLog(ByteBuffer buf) {

        LogUtils.writeInt(buf, totalCount);
        LogUtils.writeInt(buf, totalSize);
        LogUtils.writeInt(buf, totalINCount);
        LogUtils.writeInt(buf, totalINSize);
        LogUtils.writeInt(buf, totalLNCount);
        LogUtils.writeInt(buf, totalLNSize);
        LogUtils.writeInt(buf, -1); /* Reserved for obsoleteINCount. */
        LogUtils.writeInt(buf, obsoleteLNCount);
    }

    public void readFromLog(ByteBuffer buf) {

        totalCount = LogUtils.readInt(buf);
        totalSize = LogUtils.readInt(buf);
        totalINCount = LogUtils.readInt(buf);
        totalINSize = LogUtils.readInt(buf);
        totalLNCount = LogUtils.readInt(buf);
        totalLNSize = LogUtils.readInt(buf);
        LogUtils.readInt(buf); /* Reserved for obsoleteINCount. */
        obsoleteLNCount = LogUtils.readInt(buf);
    }

    public String toString() {

        StringBuffer buf = new StringBuffer();
        buf.append("<summary totalCount=\"");
        buf.append(totalCount);
        buf.append("\" totalSize=\"");
        buf.append(totalSize);
        buf.append("\" totalINCount=\"");
        buf.append(totalINCount);
        buf.append("\" totalINSize=\"");
        buf.append(totalINSize);
        buf.append("\" totalLNCount=\"");
        buf.append(totalLNCount);
        buf.append("\" totalLNSize=\"");
        buf.append(totalLNSize);
        buf.append("\" obsoleteLNCount=\"");
        buf.append(obsoleteLNCount);
        buf.append("\"/>");
        return buf.toString();
    }
}
