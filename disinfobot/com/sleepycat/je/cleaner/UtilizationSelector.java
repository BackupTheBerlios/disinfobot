/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: UtilizationSelector.java,v 1.1 2004/11/22 18:27:58 kate Exp $
*/

package com.sleepycat.je.cleaner;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.utilint.DbLsn;

/**
 * Selects files for cleaning based on the utilization profile, and handles
 * retries when cleaning cannot be completed.
 */
class UtilizationSelector implements FileSelector {

    /*
     * If we retry a file for maxRetries without success, we'll do normal
     * cleaning for maxRestartRetries and then start retrying again.  But if
     * while in wait-after-retry mode we fail to clean a different file, it
     * becomes the retry file and we discard the former retry file info.
     */

    private UtilizationProfile profile;
    private FileRetryInfo retryFile;
    private int maxRetries;
    private int maxRestartRetries;
    private int retryCycles;
    private boolean waitAfterRetry;

    UtilizationSelector(EnvironmentImpl env, UtilizationProfile profile)
        throws DatabaseException {

        this.profile = profile;

        maxRetries = env.getConfigManager().getInt
            (EnvironmentParams.CLEANER_RETRIES);
        maxRestartRetries = env.getConfigManager().getInt
            (EnvironmentParams.CLEANER_RESTART_RETRIES);
    }

    public FileRetryInfo getFileToClean(Set excludeFiles, boolean aggressive)
        throws DatabaseException {

        /* Retry a file that wasn't completed earlier. */
        if (retryFile != null &&
            !waitAfterRetry &&
            !excludeFiles.contains(retryFile.getFileNumber())) {
            return retryFile;
        }

        Set useExcludeFiles = excludeFiles;
        if (retryFile != null) {
            useExcludeFiles = new HashSet(excludeFiles);
            useExcludeFiles.add(retryFile.getFileNumber());
        }

        /* Select a file for cleaning from the profile. */
        Long file = profile.getBestFileForCleaning(useExcludeFiles,
                                                   aggressive);
        if (file != null) {
            return new RetryInfo(file);
        } else {
            return null;
        }
    }

    /** 
     * When file processing is complete, setup for retries next time.
     */
    private void onEndFile(RetryInfo file, boolean deleted) {

        if (deleted) {
            /*
             * We sucessfully cleaned a file.
             */
            if (file == retryFile) {
                assert !waitAfterRetry;
                /*
                 * If we cleaned the retry file, forget about it.
                 */
                resetRetryInfo();
            } else if (waitAfterRetry) {
                assert retryFile != null;
                /*
                 * We're in wait-after-retry mode, bump the count.  If we've
                 * exceeded the maxRestartRetries, setup to retry again next
                 * time.
                 */
                retryCycles += 1;
                if (retryCycles >= maxRestartRetries) {
                    waitAfterRetry = false;
                    retryCycles = 0;
                }
            } else {
                assert retryFile == null;
            }
        } else if (retryFile == file) {
            /*
             * We failed to clean the retry file, bump the count.  If we've
             * exeeded maxRetries, setup for wait-after-retry next time.
             */
            retryCycles += 1;
            if (retryCycles >= maxRetries) {
                waitAfterRetry = true;
                retryCycles = 0;
            }
        } else {
            /*
             * We failed to clean a file that is not the retry file.  Retry
             * this file next time and forget about another retry file that
             * failed earlier, if any.
             */
            resetRetryInfo();
            retryFile = file;
        }
    }

    /**
     * Resets retry info when the retry file is successfully cleaned.
     */
    private void resetRetryInfo() {
        retryFile = null;
        retryCycles = 0;
        waitAfterRetry = false;
    }

    private class RetryInfo implements FileRetryInfo {

        private Long fileNum;
        private DbLsn firstUnprocessedLsn;
        private List pendingLsns;
        private boolean fullyProcessed;

        RetryInfo(Long fileNum) {
            this.fileNum = fileNum;
            pendingLsns = new ArrayList();
        }

        public Long getFileNumber() {
            return fileNum;
        }

        public void beginFileProcessing() {
        }

        public void endProcessing(boolean deleted) {

            onEndFile(this, deleted);
        }

        public boolean canFileBeDeleted() {

            if (fullyProcessed && pendingLsns.isEmpty()) {
                return true;
            } else {
                return false;
            }
        }

        public void setFileFullyProcessed() {

            fullyProcessed = true;
        }

        public boolean isFileFullyProcessed() {

            return fullyProcessed;
        }

        public void setFirstUnprocessedLsn(DbLsn lsn) {

            assert lsn.getFileNumber() == fileNum.longValue();
            firstUnprocessedLsn = lsn;
        }

        public DbLsn getFirstUnprocessedLsn() {

            return firstUnprocessedLsn;
        }

        public DbLsn[] getPendingLsns() {

            if (pendingLsns.size() > 0) {
                DbLsn[] lsns = new DbLsn[pendingLsns.size()];
                pendingLsns.toArray(lsns);
                return lsns;
            } else {
                return null;
            }
        }

        public boolean isObsoleteLN(DbLsn lsn, long nodeId) {

            assert lsn.getFileNumber() == fileNum.longValue();
            return false; // We don't currently do this optimization.
        }

        public void setObsoleteLN(DbLsn lsn, long nodeId) {

            assert lsn.getFileNumber() == fileNum.longValue();
            pendingLsns.remove(lsn);
        }

        public void setPendingLN(DbLsn lsn, long nodeId) {

            assert lsn.getFileNumber() == fileNum.longValue();
            pendingLsns.add(lsn);
        }
    }
}
