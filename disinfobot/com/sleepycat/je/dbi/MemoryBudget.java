/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: MemoryBudget.java,v 1.1 2004/11/22 18:27:59 kate Exp $
*/

package com.sleepycat.je.dbi;

import java.util.Iterator;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.latch.Latch;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.IN;

/**
 * MemoryBudget calculates the available memory for JE and how to apportion
 * it between cache and log buffers. It is meant to centralize all memory 
 * calculations. Objects that ask for memory budgets should get settings from
 * this class, rather than using the configuration parameter values directly.
 */

public class MemoryBudget {

    /*
     * Object overheads. These are set statically with advance measurements.
     * Java doesn't provide a way of assessing object size dynamically. These
     * overheads will not be precise, but are close enough to let the system
     * behave predictably.
     */
    public final static long LN_OVERHEAD = 24;
    public final static long IN_FIXED_OVERHEAD = 235;
    public final static long BIN_FIXED_OVERHEAD = 400;
    public final static long BYTE_ARRAY_OVERHEAD = 12;
    public final static long OBJECT_OVERHEAD = 8;
    public final static long KEY_OVERHEAD = 32;
    public final static long LSN_SIZE = 16;
    public final static long ARRAY_ITEM_OVERHEAD = 4;

    private final static long N_64MB = (1 << 26);

    /* 
     * environmentMemoryUsage is the per-environment unlatched, inaccurate
     * count used to trigger eviction.
     */
    private volatile long cacheMemoryUsage;

    /* 
     * Memory available to JE, based on je.maxMemory and the memory available
     * to this process.
     */
    private long maxMemory;
                           
    /* Memory available to log buffers. */
    private long logBufferBudget;

    /* Memory to hold internal nodes, controlled by the evictor. */
    private long treeBudget;
    
    /* 
     * Overheads that are a function of node capacity.
     */
    private long inOverhead;
    private long binOverhead;

    private EnvironmentImpl envImpl;

    MemoryBudget(EnvironmentImpl envImpl,
                 DbConfigManager configManager) 
        throws DatabaseException {

        this.envImpl = envImpl;

        /* 
         * Calculate the total memory allotted to JE.
         * 1. If je.maxMemory is specified, use that. Check that it's
         * not more than the jvm memory.
         * 2. Otherwise, take je.maxMemoryPercent * JVM max memory.
         */
        maxMemory = configManager.getLong(EnvironmentParams.MAX_MEMORY);
        long jvmMemory = getRuntimeMaxMemory();

	/*
	 * Runtime.maxMemory() may return Long.MAX_VALUE if there is no
	 * inherent limit.
	 */
	if (jvmMemory == Long.MAX_VALUE) {
	    jvmMemory = N_64MB;
	}

        if (maxMemory != 0) {
            /* Application specified a cache size number, validate it. */
            if (jvmMemory < maxMemory) {
                throw new IllegalArgumentException
                    (EnvironmentParams.MAX_MEMORY.getName() +
                     " has a value of " + maxMemory +
                     " but the JVM is only configured for " +
                     jvmMemory +
                     ". Consider using je.maxMemoryPercent.");
            }
        } else {
            int maxMemoryPercent =
                configManager.getInt(EnvironmentParams.MAX_MEMORY_PERCENT);
            maxMemory = (maxMemoryPercent * jvmMemory)/100;
        }

        /*
         * Calculate the memory budget for log buffering.
	 * If the LOG_MEM_SIZE parameter is not set, start by using
         * 7% (1/16th) of the cache size. If it is set, use that
         * explicit setting.
         * 
         * No point in having more log buffers than the maximum size. If
         * this starting point results in overly large log buffers,
         * reduce the log buffer budget again.
         */
        logBufferBudget =
            configManager.getLong(EnvironmentParams.LOG_MEM_SIZE);	    
        if (logBufferBudget == 0) {
	    logBufferBudget = maxMemory >> 4;
	} else if (logBufferBudget > maxMemory/2) {
            logBufferBudget = maxMemory/2;
        }

        /* 
         * We have a first pass at the log buffer budget. See what
         * size log buffers result. Don't let them be too big, it would
         * be a waste.
         */
        int numBuffers =
	    configManager.getInt(EnvironmentParams.NUM_LOG_BUFFERS);
        long startingBufferSize = logBufferBudget / numBuffers; 
        int logBufferSize =
            configManager.getInt(EnvironmentParams.LOG_BUFFER_MAX_SIZE);
        if (startingBufferSize > logBufferSize) {
            startingBufferSize = logBufferSize;
            logBufferBudget = numBuffers * startingBufferSize;
        }

        /* 
         * The remainder of the memory is left for tree nodes.
         */
        treeBudget = maxMemory - logBufferBudget;

        /*
         * Calculate IN and BIN overheads, which are a function of
         * capacity. These values are stored in this class so that they
         * can be calculated once per environment. The logic to do the
         * calculations is left in the respective node classes so it
         * can be properly the domain of those objects.
         */
        inOverhead = IN.computeOverhead(configManager);
        binOverhead = BIN.computeOverhead(configManager);
    }

    /**
     * Returns Runtime.maxMemory(), accounting for a MacOS bug.
     * Used by unit tests as well as by this class.
     */
    public static long getRuntimeMaxMemory() {

        long jvmMemory = Runtime.getRuntime().maxMemory();

        /*
         * Workaround a MacOS bug that reports 64mb more than the actual max
         * memory available.
         */
        if (jvmMemory != Long.MAX_VALUE &&
            jvmMemory > N_64MB &&
            "Mac OS X".equals(System.getProperty("os.name")) &&
            "1.4.2_03".equals(System.getProperty("java.version"))) {
            jvmMemory -= N_64MB;
        }

        return jvmMemory;
    }

    /**
     * Initialize the starting environment memory state 
     */
    void initCacheMemoryUsage() 
        throws DatabaseException {

        long totalSize = 0;
        INList inList = envImpl.getInMemoryINs();

        inList.latchMajor();
        try {
            Iterator iter = inList.iterator();
            while (iter.hasNext()) {
                IN in = (IN) iter.next();
                long size = in.getInMemorySize();
                totalSize += size;
            }
        } finally {
            inList.releaseMajorLatch();
        }
        assert Latch.countLatchesHeld() == 0;
        cacheMemoryUsage = totalSize;
    }

    /**
     * Update the environment wide count, wake up the evictor if necessary.
     * @param increment note that increment may be negative.
     */
    public void updateCacheMemoryUsage(long increment) {
        cacheMemoryUsage += increment;
        if (cacheMemoryUsage > treeBudget) {
            envImpl.alertEvictor();
        }
    }

    public long accumulateNewUsage(IN in, long newSize) {
        return in.getInMemorySize() + newSize;
    }

    public void refreshCacheMemoryUsage(long newSize) {
        cacheMemoryUsage = newSize;
    }

    public long getCacheMemoryUsage() {
        return cacheMemoryUsage;
    }

    /**
     * @return
     */
    public long getLogBufferBudget() {
        return logBufferBudget;
    }

    /**
     * @return
     */
    public long getMaxMemory() {
        return maxMemory;
    }

    /**
     * @return
     */
    public long getTreeBudget() {
        return treeBudget;
    }

    public long getINOverhead() {
        return inOverhead;
    }

    public long getBINOverhead() {
        return binOverhead;
    }

    void loadStats(StatsConfig config,
                   EnvironmentStatsInternal stats) {
        stats.setCacheDataBytes(getCacheMemoryUsage());
    }
}
