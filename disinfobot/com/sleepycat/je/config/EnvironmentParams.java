/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: EnvironmentParams.java,v 1.1 2004/11/22 18:27:59 kate Exp $
*/

package com.sleepycat.je.config;

import java.io.File;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;

/**
 * Javadoc for this public class is generated
 * via the doc templates in the doc_src directory.
 */
public class EnvironmentParams {

    /*
     * The map of supported environment parameters where the key is parameter 
     * name and the data is the configuration parameter object. Put first,
     * before any declarations of ConfigParams.
     */
    public final static Map SUPPORTED_PARAMS = new HashMap();

    /*
     * Environment
     */
    public static final LongConfigParam MAX_MEMORY =
        new LongConfigParam("je.maxMemory",
			    new Long(1024), // min
			    null,           // max
			    new Long(0),    // default uses je.maxMemoryPercent
                            false,          // mutable
      "# Specify the cache size in bytes, as an absolute number. The system\n"+
      "# attempts to stay within this budget and will evict database\n" +
      "# objects when it comes within a prescribed margin of the limit.\n" +
      "# By default, this parameter is 0 and JE instead sizes the cache\n" +
      "# proportionally to the memory available to the JVM, based on\n"+
      "# je.maxMemoryPercent.\n");

    public static final IntConfigParam MAX_MEMORY_PERCENT =
        new IntConfigParam("je.maxMemoryPercent",
			   new Integer(10),   // min 
			   new Integer(90),   // max
			   new Integer(60),   // default
                           false,             // mutable
     "# By default, JE sizes the cache as a percentage of the maximum\n" +
     "# memory available to the JVM. For example, if the JVM is\n" + 
     "# started with -Xmx128M, the cache size will be\n" +
     "#           je.maxMemoryPercent * 128M\n" +
     "# Setting je.maxMemory to an non-zero value will override\n" +
     "# je.maxMemoryPercent");
     
    public static final BooleanConfigParam ENV_RECOVERY =
        new BooleanConfigParam("je.env.recovery",
			       true, // default
                               false,// mutable
     "# If true, an environment is created with recovery and the related\n" +
     "# daemons threads enabled.");
     
    public static final BooleanConfigParam ENV_RUN_INCOMPRESSOR =
        new BooleanConfigParam("je.env.runINCompressor",
			       true, // default
                               true, // mutable
     "# If true, starts up the INCompressor.");
     
    public static final BooleanConfigParam ENV_RUN_EVICTOR =
        new BooleanConfigParam("je.env.runEvictor",
			       true, // default
                               true, // mutable
     "# If true, starts up the evictor.");
     
    public static final BooleanConfigParam ENV_RUN_CHECKPOINTER =
        new BooleanConfigParam("je.env.runCheckpointer",
			       true, // default
                               true, // mutable
     "# If true, starts up the checkpointer.");

    public static final BooleanConfigParam ENV_RUN_CLEANER =
        new BooleanConfigParam("je.env.runCleaner",
			       true, // default
                               true, // mutable
     "# If true, starts up the cleaner.");

    public static final BooleanConfigParam ENV_CHECK_LEAKS =
        new BooleanConfigParam("je.env.checkLeaks",
			       true, // default
                               false,// mutable
     "# Debugging support: check leaked locks and txns at env close.");

    public static final BooleanConfigParam ENV_FORCED_YIELD =
        new BooleanConfigParam("je.env.forcedYield",
			       false, // default
                               false,// mutable
     "# Debugging support: call Thread.yield() at strategic points.");

    public static final BooleanConfigParam ENV_INIT_TXN =
        new BooleanConfigParam("je.env.isTransactional",
			       false, // default
                               false,// mutable
     "# If true, create the environment w/ transactions.");

    public static final BooleanConfigParam ENV_RDONLY =
        new BooleanConfigParam("je.env.isReadOnly",
			       false, // default
                               false, // mutable
     "# If true, create the environment read only.");

    public static final BooleanConfigParam ENV_FAIR_LATCHES =
        new BooleanConfigParam("je.env.fairLatches",
			       false, // default
                               false, // mutable
     "# If true, use latches instead of synchronized blocks to\n" +
     "# implement the lock table and log write mutexes. Latches require\n" +
     "# that threads queue to obtain the mutex in question and\n" +
     "# therefore guarantee that there will be no mutex starvation, but \n" +
     "# do incur a performance penalty. Latches should not be necessary in\n"+
     "# most cases, so synchronized blocks are the default. An application\n" +
     "# that puts heavy load on JE with threads with very different thread\n"+
     "# priorities might find it useful to use latches");
    
    /*
     * Database Logs
     */
    public static final LongConfigParam LOG_MEM_SIZE = 
        new LongConfigParam("je.log.totalBufferBytes",
			    new Long(25),      // min
			    null,              // max
                            new Long(0), //by default computed from je.maxMemory
                            false,             // mutable
     "# The total memory taken by log buffers, in bytes. If 0, use\n" +
     "# 7% of je.maxMemory");

    public static final IntConfigParam NUM_LOG_BUFFERS = 
        new IntConfigParam("je.log.numBuffers",
			   new Integer(2),  // min
			   null,            // max
			   new Integer(5),  // default
                           false,           // mutable
     "# The number of JE log buffers"); 

    public static final IntConfigParam LOG_BUFFER_MAX_SIZE =
        new IntConfigParam("je.log.bufferSize",
			   new Integer(1<<10),  // min
			   null,                // max
			   new Integer(1<<20),  // default
                           false,               // mutable
     "# maximum starting size of a JE log buffer");

    public static final IntConfigParam LOG_FAULT_READ_SIZE = 
        new IntConfigParam("je.log.faultReadSize",
			   new Integer(32),   // min
			   null,              // max
			   new Integer(2048), // default
                           false,             // mutable
     "# The buffer size for faulting in objects from disk, in bytes.");

    public static final IntConfigParam LOG_ITERATOR_READ_SIZE = 
        new IntConfigParam("je.log.iteratorReadSize",
			   new Integer(128),  // min
			   null,              // max
			   new Integer(8192), // default
                           false,             // mutable
     "# The read buffer size for log iterators, which are used when\n" +
     "# scanning the log during activities like log cleaning and\n" +
     "# environment open, in bytes. This may grow as the system encounters\n" +
     "# larger log entries\n");


    public static final IntConfigParam LOG_ITERATOR_MAX_SIZE = 
        new IntConfigParam("je.log.iteratorMaxSize",
			   new Integer(128),  // min
			   null,              // max
			   new Integer(16777216), // default
                           false,             // mutable
     "# The maximum read buffer size for log iterators, which are used\n" +
     "# when scanning the log during activities like log cleaning\n" +
     "# and environment open, in bytes.\n");
     
    public static final LongConfigParam LOG_FILE_MAX =
        new LongConfigParam("je.log.fileMax",
			    new Long(64),          // min
			    new Long(4294967296L), // max
			    new Long(10000000),    // default
                            false,                 // mutable
     "# The maximum size of each individual JE log file, in bytes.");
     
    public static final BooleanConfigParam LOG_CHECKSUM_READ =
        new BooleanConfigParam("je.log.checksumRead",
			       true,               // default
                               false,              // mutable
     "# If true, perform a checksum check when reading entries from log.");

     
    public static final BooleanConfigParam LOG_MEMORY_ONLY =
        new BooleanConfigParam("je.log.memOnly",
			       false,              // default
                               false,              // mutable
     "# If true, operates in an in-memory fashion without\n" +
     "# flushing the log to disk. The system operates until it runs\n" +
     "# out of memory, in which case a java.lang.OutOfMemory error is\n" +
     "# thrown.");

    public static final IntConfigParam LOG_FILE_CACHE_SIZE = 
        new IntConfigParam("je.log.fileCacheSize",
			   new Integer(3),    // min
			   null,              // max
			   new Integer(100),  // default
                           false,             // mutable
     "# The size of the file handle cache.");

    public static final LongConfigParam LOG_FSYNC_TIMEOUT =
        new LongConfigParam("je.log.fsyncTimeout",
                            new Long(10000L),  // min
                            null,              // max
                            new Long(500000L), // default
                            false,             // mutable
           "# Timeout limit for group file sync, in microseconds.");
     
    public static final BooleanConfigParam LOG_DIRECT_NIO =
        new BooleanConfigParam("je.log.directNIO",
			       false,              // default
                               false,              // mutable
     "# If true (the default) direct NIO buffers are used.\n" +
     "# It may be desirable to disable direct buffers on some platforms.");

    
    /* 
     * Tree
     */
    public static final IntConfigParam NODE_MAX =
        new IntConfigParam("je.nodeMaxEntries",
			   new Integer(4),     // min
			   new Integer(32767), // max
			   new Integer(128),   // default
                           false,              // mutable
     "# The maximum number of entries in a internal btree node.");

    public static final IntConfigParam BIN_MAX_DELTAS =
        new IntConfigParam("je.tree.maxDelta",
			   new Integer(0),    // min 
			   new Integer(100),  // max
			   new Integer(10),   // default
                           false,             // mutable
     "# After this many deltas, logs a full version.");
     
    public static final IntConfigParam BIN_DELTA_PERCENT =
        new IntConfigParam("je.tree.binDelta",
			   new Integer(0),    // min 
			   new Integer(75),   // max
			   new Integer(25),   // default
                           false,             // mutable
     "# If less than this percentage of entries are changed on a BIN,\n" +
     "# logs a delta instead of a full version.");

    /*
     * IN Compressor
     */
    public static final LongConfigParam COMPRESSOR_WAKEUP_INTERVAL =
        new LongConfigParam("je.compressor.wakeupInterval",
			    new Long(1000000),     // min
			    new Long(4294967296L), // max
			    new Long(5000000),     // default
                            false,                 // mutable
     "# The compressor wakeup interval in microseconds.");
     
    public static final IntConfigParam COMPRESSOR_RETRY =
        new IntConfigParam("je.compressor.deadlockRetry",
			   new Integer(0),                // min
			   new Integer(Integer.MAX_VALUE),// max
			   new Integer(3),                // default
                           false,                         // mutable
     "# Number of times to retry a compression run if a deadlock occurs.");

    public static final LongConfigParam COMPRESSOR_LOCK_TIMEOUT =
        new LongConfigParam("je.compressor.lockTimeout",
			    new Long(0),           // min
			    new Long(4294967296L), // max
			    new Long(500000L),     // default
                            false,                 // mutable
     "# The lock timeout for compressor transactions in microseconds.");

    public static final BooleanConfigParam COMPRESSOR_PURGE_ROOT =
        new BooleanConfigParam("je.compressor.purgeRoot",
			       false, // default
                               false, // mutable
     "# If true, when the compressor encounters an empty tree, the root is nulled.");
     
    /*
     * Evictor
     */ 
    public static final IntConfigParam EVICTOR_USEMEM_FLOOR =
        new IntConfigParam("je.evictor.useMemoryFloor",
			   new Integer(50),           // min
			   new Integer(100),          // max
			   new Integer(80),           // default
                           false,                     // mutable
     "# When eviction happens, the evictor will push memory usage to this\n" +
     "# percentage of je.maxMemory.\n");

    public static final IntConfigParam EVICTOR_NODE_SCAN_PERCENTAGE =
        new IntConfigParam("je.evictor.nodeScanPercentage",
			   new Integer(1),           // min
			   new Integer(100),         // max
			   new Integer(10),          // default
                           false,                    // mutable
     "# The evictor percentage of total nodes to scan per wakeup."); 

    public static final
	IntConfigParam EVICTOR_EVICTION_BATCH_PERCENTAGE =
        new IntConfigParam("je.evictor.evictionBatchPercentage",
			   new Integer(1),           // min
			   new Integer(100),         // max
			   new Integer(10),          // default
                           false,                    // mutable
     "# The evictor percentage of scanned nodes to evict per wakeup."); 

    public static final IntConfigParam EVICTOR_RETRY =
        new IntConfigParam("je.evictor.deadlockRetry",
			   new Integer(0),                // min
			   new Integer(Integer.MAX_VALUE),// max
			   new Integer(3),                // default
                           false,                         // mutable
     "# The number of times to retry the evictor if it runs into a deadlock."); 

    /*
     * Checkpointer
     */
    public static final LongConfigParam CHECKPOINTER_BYTES_INTERVAL =
        new LongConfigParam("je.checkpointer.bytesInterval",
                            new Long(0),                 // min
                            new Long(Long.MAX_VALUE),    // max
                            new Long(20000000),          // default
                            false,                       // mutable
     "# Ask the checkpointer to run every time we write this many bytes\n" +
     "# to the log. If set, supercedes je.checkpointer.wakeupInterval. To\n" +
     "# use time based checkpointing, set this to 0.");

    public static final LongConfigParam CHECKPOINTER_WAKEUP_INTERVAL =
        new LongConfigParam("je.checkpointer.wakeupInterval",
			    new Long(1000000),     // min
			    new Long(4294967296L), // max
			    new Long(0),           // default
                            false,                 // mutable
     "# The checkpointer wakeup interval in microseconds. By default, this\n"+
     "# is inactive and we wakeup the checkpointer as a function of the\n" +
     "# number of bytes written to the log. (je.checkpointer.bytesInterval)");

    public static final IntConfigParam CHECKPOINTER_RETRY =
        new IntConfigParam("je.checkpointer.deadlockRetry",
			   new Integer(0),                 // miyn
			   new Integer(Integer.MAX_VALUE), // max
			   new Integer(3),                 // default
                           false,                          // mutable
     "# The number of times to retry a checkpoint if it runs into a deadlock."); 

    /*
     * Cleaner
     */

    public static final IntConfigParam CLEANER_MIN_UTILIZATION =
        new IntConfigParam("je.cleaner.minUtilization",
			   new Integer(0),           // min
			   new Integer(90),          // max
			   new Integer(50),          // default
                           false,                    // mutable
     "# The cleaner will keep the disk space utilization percentage above\n"+
     "# this value. The default is set to 50 percent.");

    public static final LongConfigParam CLEANER_BYTES_INTERVAL =
        new LongConfigParam("je.cleaner.bytesInterval",
                            new Long(0),                 // min
                            new Long(Long.MAX_VALUE),    // max
                            new Long(0),                 // default
                            false,                       // mutable
     "# The cleaner checks disk utilization every time we write this many\n" +
     "# bytes to the log.  If zero (and by default) it is set to the\n" +
     "# je.log.fileMax value divided by four.");

    public static final IntConfigParam CLEANER_DEADLOCK_RETRY =
        new IntConfigParam("je.cleaner.deadlockRetry",
			   new Integer(0),                // min
			   new Integer(Integer.MAX_VALUE),// max
			   new Integer(3),                // default
                           false,                         // mutable
     "# The number of times to retry cleaning if a deadlock occurs.\n" +
     "# The default is set to 3."); 

    public static final LongConfigParam CLEANER_LOCK_TIMEOUT =
        new LongConfigParam("je.cleaner.lockTimeout",
			    new Long(0),            // min
			    new Long(4294967296L),  // max
			    new Long(500000L),      // default
                            false,                  // mutable
     "# The lock timeout for cleaner transactions in microseconds.\n" +
     "# The default is set to 0.5 seconds."); 
      
    public static final BooleanConfigParam CLEANER_REMOVE =
        new BooleanConfigParam("je.cleaner.expunge",
			       true, // default
                               false,// mutable
     "# If true, the cleaner deletes log files after successful cleaning.\n" +
     "# If false, the cleaner changes log file extensions to .DEL\n" +
     "# instead of deleting them. The default is set to true.");

    public static final IntConfigParam CLEANER_MIN_FILES_TO_DELETE =
        new IntConfigParam("je.cleaner.minFilesToDelete",
			   new Integer(1),           // min
			   new Integer(1000000),     // max
			   new Integer(5),           // default
                           false,                    // mutable
     "# The minimum number of files cleaned after which a full checkpoint\n" +
     "# will be performed and the files will be deleted.  Increasing this\n" +
     "# value can increase performance but also increases the risk that\n" +
     "# the cleaner may become backlogged after a crash.");

    public static final IntConfigParam CLEANER_RETRIES =
        new IntConfigParam("je.cleaner.retries",
			   new Integer(0),           // min
			   new Integer(1000),        // max
			   new Integer(10),          // default
                           false,                    // mutable
     "# The number of times to retry a file when cleaning fails because\n" +
     "# the application is writing to the file.  The default is set to 10.");

    public static final IntConfigParam CLEANER_RESTART_RETRIES =
        new IntConfigParam("je.cleaner.restartRetries",
			   new Integer(0),           // min
			   new Integer(1000),        // max
			   new Integer(5),           // default
                           false,                    // mutable
     "# The number of files to clean after retries is exceeded,\n" +
     "# before retrying the file again.  The default is set to 5.");

    public static final IntConfigParam CLEANER_MIN_AGE =
        new IntConfigParam("je.cleaner.minAge",
			   new Integer(1),           // min
			   new Integer(1000),        // max
			   new Integer(2),           // default
                           false,                    // mutable
     "# The minimum age of a file (number of files between it and the\n" +
     "# active file) to qualify it for cleaning under any conditions.\n" +
     "# The default is set to 2.");

    public static final IntConfigParam CLEANER_DISK_SPACE_TOLERANCE =
        new IntConfigParam("je.cleaner.maxDiskSpaceTolerance",
			   new Integer(1),           // min
			   new Integer(100),         // max
			   new Integer(5),           // default
                           false,                    // mutable
     "# The percentage of the je.maxDiskSpace setting at which aggressive\n" +
     "# cleaning measures will be taken.\n" +
     "# ** This is an experimental setting and is unsupported. **");

    public static final IntConfigParam CLEANER_OBSOLETE_AGE =
        new IntConfigParam("je.cleaner.obsoleteAge",
			   new Integer(1),           // min
			   new Integer(100),         // max
			   new Integer(100),         // default
                           false,                    // mutable
     "# The log file age, from 1 to 100, at which internal nodes are\n" +
     "# considered to be obsolete.\n" +
     "# ** This is an experimental setting and is unsupported. **");
     
    public static final LongConfigParam MAX_DISK_SPACE =
        new LongConfigParam("je.maxDiskSpace",
			    new Long(1024),           // min
			    null,                     // max
			    new Long(Long.MAX_VALUE), // default
                            false,                    // mutable
      "# The maximum disk space that may be used by the JE environment.\n" +
      "# ** This is an experimental setting and is unsupported. **");

    /*
     * Transactions
     */
    public static final LongConfigParam LOCK_TIMEOUT =
        new LongConfigParam("je.lock.timeout",
			    new Long(0),           // min
			    new Long(4294967296L), // max
			    new Long(500000L),     // default
                            false,                 // mutable
     "# The lock timeout in microseconds."); 

     
    public static final LongConfigParam TXN_TIMEOUT =
        new LongConfigParam("je.txn.timeout",
			    new Long(0),           // min
			    new Long(4294967296L), // max_value
			    new Long(0),           // default
                            false,                 // mutable
   "# The transaction timeout, in microseconds. A value of 0 means no limit.");

    /*
     * Debug tracing system
     */
    public static final BooleanConfigParam JE_LOGGING_FILE =
        new BooleanConfigParam("java.util.logging.FileHandler.on",
			       false, // default
                               false, // mutable
     "# Use FileHandler in logging system.");

    public static final BooleanConfigParam JE_LOGGING_CONSOLE =
        new BooleanConfigParam("java.util.logging.ConsoleHandler.on",
			       false, // default
                               false, // mutable
     "# Use ConsoleHandler in logging system."); 

    public static final BooleanConfigParam JE_LOGGING_DBLOG =
        new BooleanConfigParam("java.util.logging.DbLogHandler.on",
			       true, // default
                               false,// mutable
     "# Use DbLogHandler in logging system."); 

    public static final IntConfigParam JE_LOGGING_FILE_LIMIT =
        new IntConfigParam("java.util.logging.FileHandler.limit",
			   new Integer(1000),       // min
			   new Integer(1000000000), // max
			   new Integer(10000000),   // default
                           false,                   // mutable
     "# Log file limit for FileHandler."); 

    public static final IntConfigParam JE_LOGGING_FILE_COUNT =
        new IntConfigParam("java.util.logging.FileHandler.count",
			   new Integer(1),  // min
			   null,            // max
			   new Integer(10), // default
                           false,           // mutable
     "# Log file count for FileHandler."); 

    public static final ConfigParam JE_LOGGING_LEVEL =
        new ConfigParam("java.util.logging.level",
                        "INFO",
                         false,           // mutable
     "# Trace messages equal and above this level will be logged.\n" +
     "# Value should be one of the predefined java.util.logging.Level values");

    public static final ConfigParam JE_LOGGING_LEVEL_LOCKMGR =
        new ConfigParam("java.util.logging.level.lockMgr",
                        "FINE", 
                         false,           // mutable
     "# Lock manager specific trace messages will be issued at this level.\n"+
     "# Value should be one of the predefined java.util.logging.Level values");

    public static final ConfigParam JE_LOGGING_LEVEL_RECOVERY =
        new ConfigParam("java.util.logging.level.recovery",
                        "FINE", 
                         false,           // mutable
     "# Recovery specific trace messages will be issued at this level.\n"+
     "# Value should be one of the predefined java.util.logging.Level values");

    public static final ConfigParam JE_LOGGING_LEVEL_EVICTOR =
        new ConfigParam("java.util.logging.level.evictor",
                        "FINE", 
                         false,           // mutable
     "# Evictor specific trace messages will be issued at this level.\n"+
     "# Value should be one of the predefined java.util.logging.Level values");

    public static final ConfigParam JE_LOGGING_LEVEL_CLEANER =
        new ConfigParam("java.util.logging.level.cleaner",
                        "FINE", 
                         false,           // mutable
     "# Cleaner specific detailed trace messages will be issued at this\n" +
     "# level. The Value should be one of the predefined \n" +
     "# java.util.logging.Level values");

    
    /*
     * Create a sample je.properties file.
     */
    public static void main(String argv[]) {
        if (argv.length != 1) {
            throw new IllegalArgumentException("Usage: EnvironmentParams " +
                                               "<samplePropertyFile>");
        }

        try {
            FileWriter exampleFile = new FileWriter(new File(argv[0]));
            TreeSet paramNames = new TreeSet(SUPPORTED_PARAMS.keySet());
            Iterator iter = paramNames.iterator();
            exampleFile.write
		("####################################################\n" +
		 "# Example Berkeley DB, Java Edition property file\n" +
		 "# Each parameter is set to its default value\n" +
		 "####################################################\n\n");
            
            while (iter.hasNext()) {
                String paramName =(String) iter.next();
                ConfigParam param =
                    (ConfigParam) SUPPORTED_PARAMS.get(paramName);
                exampleFile.write(param.getDescription() + "\n");
                String extraDesc = param.getExtraDescription();
                if (extraDesc != null) {
                    exampleFile.write(extraDesc + "\n");
                }
                exampleFile.write("#" + param.getName() + "=" +
                                  param.getDefault() +
                                  "\n# (mutable at run time: " +
                                  param.isMutable() +
                                  ")\n\n");
            }
            exampleFile.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
    
    /*
     * Add a configuration parameter to the set supported by an 
     * environment.
     */
    static void addSupportedParam(ConfigParam param) {
        SUPPORTED_PARAMS.put(param.getName(), param);
    }
}
