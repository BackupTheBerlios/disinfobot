/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: RotationSelector.java,v 1.1 2004/11/22 18:27:58 kate Exp $
*/

package com.sleepycat.je.cleaner;

import java.util.Set;

import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.FileManager;
import com.sleepycat.je.utilint.DbLsn;

/**
 * A file selector that rotates through all files that can be cleaned, starting
 * with the first/oldest file available. It moves forward in age sequence, and
 * wraps back to the first file when a non-cleanable file is encountered.
 */
class RotationSelector implements FileSelector {

    private EnvironmentImpl env;
    private Long previousFileNum;
    private Long currentFileNum;

    /**
     * Creates a rotation file selector.
     */
    RotationSelector(EnvironmentImpl env) {

        assert env != null;
        this.env = env;
    }

    /**
     * Gets the next file in rotation to be cleaned.  If endFileProcessing was
     * not called on the last cycle, the same file will be returned again.
     */
    public FileRetryInfo getFileToClean(Set excludeFiles, boolean aggressive) {

        if (currentFileNum == null) {
            moveToNextFile();
        }
        if (currentFileNum != null) {
            return new RetryInfo(currentFileNum);
        } else {
            return null;
        }
    }

    /**
     * Sets currentFileNum to the next file to clean, or to null if no files
     * can be cleaned.
     */
    private void moveToNextFile() {

        DbLsn firstActiveLsn = env.getCheckpointer().getFirstActiveLsn();
        if (firstActiveLsn == null) {
            return;
        }
        long firstActiveFileNum = firstActiveLsn.getFileNumber();
        FileManager fileManager = env.getFileManager();
        Long nextTarget;
        if (previousFileNum == null) {
            nextTarget = fileManager.getFirstFileNum();
        } else {
            nextTarget =
                fileManager.getFollowingFileNum(previousFileNum.longValue(),
                                                true);
            if (nextTarget == null ||
                nextTarget.longValue() >= firstActiveFileNum) {
                nextTarget = fileManager.getFirstFileNum();
            }
        }
        if (nextTarget != null &&
            nextTarget.longValue() < firstActiveFileNum) {
            currentFileNum = nextTarget;
        }
    }

    private class RetryInfo implements FileRetryInfo {

        private Long fileNum;
        private boolean fullyProcessed;
        private boolean hasPendingLNs;

        RetryInfo(Long fileNum) {
            this.fileNum = fileNum;
        }

        public Long getFileNumber() {
            return fileNum;
        }

        /**
         * Bumps the rotation to the next file.
         */
        public void endProcessing(boolean deleted) {

            previousFileNum = currentFileNum;
            currentFileNum = null;
        }

        /**
         * Returns whether any LSNs were unprocessed or were given a
         * non-obsolete disposition since beginFileProcessing was called.
         */
        public boolean canFileBeDeleted() {

            return !hasPendingLNs && fullyProcessed;
        }

        public void setFileFullyProcessed() {

            fullyProcessed = true;
        }

        public boolean isFileFullyProcessed() {

            return fullyProcessed;
        }

        public void setFirstUnprocessedLsn(DbLsn lsn) {

            assert lsn.getFileNumber() == fileNum.longValue();
        }

        public DbLsn getFirstUnprocessedLsn() {

            return null;
        }

        public DbLsn[] getPendingLsns() {

            return null;
        }

        public boolean isObsoleteLN(DbLsn lsn, long nodeId) {

            assert lsn.getFileNumber() == fileNum.longValue();
            return false;
        }

        public void setObsoleteLN(DbLsn lsn, long nodeId) {

            assert lsn.getFileNumber() == fileNum.longValue();
        }

        /**
         * Prevents file from being deleted.
         */
        public void setPendingLN(DbLsn lsn, long nodeId) {

            assert lsn.getFileNumber() == fileNum.longValue();
            hasPendingLNs = true;
        }
    }
}
