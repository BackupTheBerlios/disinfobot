/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: StatsFileReader.java,v 1.1 2004/11/22 18:27:58 kate Exp $
*/

package com.sleepycat.je.log;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.utilint.DbLsn;

/**
 * The StatsFileReader generates stats about the log entries read,
 * such as the count of each type of entry, the number of bytes,
 * minimum and maximum sized log entry.
 */
public class StatsFileReader extends DumpFileReader {

    /* Keyed by LogEntryType, data is EntryInfo */
    private Map entryInfoMap;
    private long totalLogBytes;
    private long totalCount;

    /* Keep stats on log composition in terms of ckpt intervals */
    private ArrayList ckptList;
    private CheckpointCounter ckptCounter;
    private DbLsn firstLsnRead;
    

    /**
     * Create this reader to start at a given LSN.
     */
    public StatsFileReader(EnvironmentImpl env,
			   int readBufferSize, 
			   DbLsn startLsn,
			   DbLsn finishLsn,
			   String entryTypes,
			   String txnIds,
			   boolean verbose)
	throws IOException, DatabaseException {

        super(env, readBufferSize, startLsn, finishLsn,
              entryTypes, txnIds, verbose);
        entryInfoMap = new TreeMap(new LogEntryTypeComparator());
        totalLogBytes = 0;
        totalCount = 0;

        ckptCounter = new CheckpointCounter();
        ckptList = new ArrayList();
        if (verbose) {
            ckptList.add(ckptCounter);
        }
    }

    /**
     * This reader collects stats about the log entry.
     */
    protected boolean processEntry(ByteBuffer entryBuffer)
        throws DatabaseException {

        /* 
         * Record various stats based on the entry header, then move
         * the buffer forward to skip ahead.
         */
        LogEntryType lastEntryType =
            LogEntryType.findType(currentEntryTypeNum,
                                  currentEntryTypeVersion);
        entryBuffer.position(entryBuffer.position() + currentEntrySize);

        /* 
         * Get the info object for it, if this is the first time it's seen,
         * create an info object and insert it.
         */
        EntryInfo info = (EntryInfo) entryInfoMap.get(lastEntryType);
        if (info == null) {
            info = new EntryInfo();
            entryInfoMap.put(lastEntryType, info);
        }
        
        /* Update counts. */
        info.count++;
        totalCount++;
        if (LogEntryType.isProvisional(currentEntryTypeVersion)) {
            info.provisionalCount++;
        }
        int size = currentEntrySize + LogManager.HEADER_BYTES;
        info.totalBytes += size;
        totalLogBytes += size;
        if ((info.minBytes==0) || (info.minBytes > size)) {
            info.minBytes = size;
        }
        if (info.maxBytes < size) {
            info.maxBytes = size;
        }

        if (verbose) {
            if (firstLsnRead == null) {
                firstLsnRead = getLastLsn();
            } 

            if (currentEntryTypeNum ==
                LogEntryType.LOG_CKPT_END.getTypeNum()) {
                /* start counting a new interval */
                ckptCounter.endCkptLsn = getLastLsn();
                ckptCounter = new CheckpointCounter();
                ckptList.add(ckptCounter);
            } else {
                ckptCounter.increment(this, currentEntryTypeNum);
            }
        }
        
        return true;
    }

    public void summarize() {
        System.out.println("Log statistics:");
        Iterator iter = entryInfoMap.entrySet().iterator();

        NumberFormat form = NumberFormat.getIntegerInstance();
        NumberFormat percentForm = NumberFormat.getInstance();
        percentForm.setMaximumFractionDigits(1);
        System.out.println(pad("type") +
                           pad("total") + 
                           pad("provisional") + 
                           pad("total") + 
                           pad("min") + 
                           pad("max") + 
                           pad("avg") + 
                           pad("entries"));

        System.out.println(pad("") +
                           pad("count") + 
                           pad("count") + 
                           pad("bytes") + 
                           pad("bytes") + 
                           pad("bytes") + 
                           pad("bytes") + 
                           pad("as % of log"));


        while (iter.hasNext()) {
            Map.Entry m = (Map.Entry) iter.next();
            EntryInfo info = (EntryInfo) m.getValue();
            StringBuffer sb = new StringBuffer();
            LogEntryType entryType =(LogEntryType) m.getKey();
            sb.append(pad(entryType.toString()));
            sb.append(pad(form.format(info.count)));
            sb.append(pad(form.format(info.provisionalCount)));
            sb.append(pad(form.format(info.totalBytes)));
            sb.append(pad(form.format(info.minBytes)));
            sb.append(pad(form.format(info.maxBytes)));
            sb.append(pad(form.format((long)(info.totalBytes/info.count))));
            double entryPercent =
                ((double)(info.totalBytes *100)/totalLogBytes);
            sb.append(pad(percentForm.format(entryPercent)));
            System.out.println(sb.toString());

            /* Print special line for LN_TX */
            if (entryType == LogEntryType.LOG_LN_TRANSACTIONAL) {
                sb = new StringBuffer();
                sb.append(pad("key/data"));
                sb.append(pad(""));
                sb.append(pad(""));
                /* LN_TX entry overhead:
                   8 bytes node id
                   1 byte boolean (whether data exists or is null)
                   4 bytes data size
                   4 bytes key size
                   4 bytes database id
                   8 bytes abort lsn
                   8 bytes txn id
                   8 bytes lastlogged lsn (backpointer for txn)
                */

                int overhead = LogManager.HEADER_BYTES + 46;
                long realTotalBytes = info.totalBytes-(info.count*overhead);
                sb.append(pad(form.format(realTotalBytes)));
                sb.append(pad(""));
                sb.append(pad(""));
                sb.append(pad(""));
                String realSize = "(" + 
                    percentForm.format((double)(realTotalBytes*100)/
                                       totalLogBytes) +
                    ")";
                sb.append(pad(realSize));

                System.out.println(sb.toString());
            }
        }
        System.out.println("\nTotal bytes in portion of log read: " +
                           form.format(totalLogBytes));
        System.out.println("Total number of entries: " +
                           form.format(totalCount));



        if (verbose) {
            summarizeCheckpointInfo();
        }
    }
    
    private String pad(String result) {
        int spaces = 14 - result.length();
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < spaces; i++) {
            sb.append(" ");
        }
        sb.append(result);
        return sb.toString();
    }

    private void summarizeCheckpointInfo() {
        System.out.println("\nPer checkpoint interval info:");
        /*
         * Print out checkpoint interval info. 
         * If the log looks like this:
         * 
         * start of log
         * ckpt1 start
         * ckpt1 end
         * ckpt2 start
         * ckpt2 end
         * end of log
         *
         * There are 3 ckpt intervals
         * start of log->ckpt1 end
         * ckpt1 end -> ckpt2 end
         * ckpt2 end -> end of log
         */
        System.out.println(pad("lnTxn") + 
                           pad("ln") + 
                           pad("mapLNTxn") + 
                           pad("mapLN") +
                           pad("end-end") +    // ckpt n-1 end -> ckpt n end
                           pad("end-start") +  // ckpt n-1 end -> ckpt n start
                           pad("start-end") +  // ckpt n start -> ckpt n end
                           pad("maxLNReplay") +
                           pad("ckptEnd"));



        long logFileMax;
        try {
            logFileMax = env.getConfigManager().getLong(
                                   EnvironmentParams.LOG_FILE_MAX);
        } catch (DatabaseException e) {
            e.printStackTrace();
            return;
        }

        Iterator iter = ckptList.iterator();
        CheckpointCounter prevCounter = null;
        NumberFormat form = NumberFormat.getInstance();
        while (iter.hasNext()) {
            CheckpointCounter c = (CheckpointCounter)iter.next();
            StringBuffer sb = new StringBuffer();

            /* Entry type counts. */
            int maxTxnLNs = c.preStartLNTxnCount + c.postStartLNTxnCount;
            sb.append(pad(form.format(maxTxnLNs)));
            int maxLNs = c.preStartLNCount + c.postStartLNCount;
            sb.append(pad(form.format(maxLNs)));
            sb.append(pad(form.format(c.preStartMapLNTxnCount +
                                      c.postStartMapLNTxnCount)));
            sb.append(pad(form.format(c.preStartMapLNCount +
                                      c.postStartMapLNCount)));

            /* Checkpoint interval distance. */
            DbLsn end = (c.endCkptLsn == null) ? getLastLsn() :
                c.endCkptLsn;
            long endToEndDistance = 0;

            FileManager fileManager = env.getFileManager();
            if (prevCounter == null) {
                endToEndDistance =
                    end.getWithCleaningDistance(fileManager,
                                                firstLsnRead,
                                                logFileMax);
            } else {
                endToEndDistance =
                    end.getWithCleaningDistance(fileManager,
                                                prevCounter.endCkptLsn,
                                                logFileMax);
            }
            sb.append(pad(form.format(endToEndDistance)));

            /* 
             * Interval between last checkpoint end and
             * this checkpoint start. 
             */
            DbLsn start = (c.startCkptLsn == null) ? getLastLsn() :
                c.startCkptLsn;
            long endToStartDistance = 0;

            if (prevCounter == null) {
                endToStartDistance =
                    start.getWithCleaningDistance(fileManager,
                                                  firstLsnRead,
                                                  logFileMax);
            } else {
                endToStartDistance =
                    start.getWithCleaningDistance(fileManager,
                                                  prevCounter.endCkptLsn,
                                                  logFileMax);
            }
            sb.append(pad(form.format(endToStartDistance)));

            /* 
             * Interval between ckpt start and ckpt end.
             */
            long startToEndDistance = 0;
            if ((c.startCkptLsn != null)  &&
                (c.endCkptLsn != null)) {
                startToEndDistance =
                    c.endCkptLsn.getWithCleaningDistance(fileManager,
                                                         c.startCkptLsn,
                                                         logFileMax);
            }
            sb.append(pad(form.format(startToEndDistance)));

            /* 
             * The maximum number of LNs to replay includes the portion
             * of LNs from checkpoint start to checkpoint end of the 
             * previous interval.
             */
            int maxReplay = maxLNs + maxTxnLNs;
            if (prevCounter != null) {
                maxReplay += prevCounter.postStartLNTxnCount;
                maxReplay += prevCounter.postStartLNCount;
            }
            sb.append(pad(form.format(maxReplay)));

            if (c.endCkptLsn == null) {
                sb.append("   ").append(getLastLsn().getNoFormatString());
            } else {
                sb.append("   ").append(c.endCkptLsn.getNoFormatString());
            }
            
            System.out.println(sb.toString());
            prevCounter = c;
        }
    }
        
    static class EntryInfo {
        public int count;
        public int provisionalCount;
        public long totalBytes;
        public int minBytes;
        public int maxBytes;

        EntryInfo() {
            count = 0;
            provisionalCount = 0;
            totalBytes = 0;
            minBytes = 0;
            maxBytes = 0;
        }
    }
        
    static class LogEntryTypeComparator implements Comparator {
	public int compare(Object o1, Object o2) {
	    if (o1 == null) {
		return -1;
	    }

	    if (o1 == null) {
		return 1;
	    }

	    if (o1 instanceof LogEntryType &&
		o2 instanceof LogEntryType) {
		Byte t1 = new Byte(((LogEntryType) o1).getTypeNum());
		Byte t2 = new Byte(((LogEntryType) o2).getTypeNum());
		return t1.compareTo(t2);
	    } else {
		throw new IllegalArgumentException
		    ("non LogEntryType passed to LogEntryType.compare");
	    }
	}
    }

    /*
     * Accumulate the count of items from checkpoint end->checkpoint end.
     */
    static class CheckpointCounter {
        public DbLsn startCkptLsn = null;
        public DbLsn endCkptLsn = null;
        public int preStartLNTxnCount;
        public int preStartLNCount;
        public int preStartMapLNTxnCount;
        public int preStartMapLNCount;
        public int postStartLNTxnCount;
        public int postStartLNCount;
        public int postStartMapLNTxnCount;
        public int postStartMapLNCount;

        public void increment(FileReader reader,  byte currentEntryTypeNum) {
            if (currentEntryTypeNum == 
                LogEntryType.LOG_CKPT_START.getTypeNum()) {
                startCkptLsn = reader.getLastLsn();
            } else if (currentEntryTypeNum == 
                       LogEntryType.LOG_LN_TRANSACTIONAL.getTypeNum()) {
                if (startCkptLsn == null) {
                    preStartLNTxnCount++;
                } else {
                    postStartLNTxnCount++;
                }
            } else if (currentEntryTypeNum == 
                       LogEntryType.LOG_LN.getTypeNum()) {
                if (startCkptLsn == null) {
                    preStartLNCount++;
                } else {
                    postStartLNCount++;
                }
            } else if (currentEntryTypeNum == 
                       LogEntryType.LOG_MAPLN.getTypeNum()) {
                if (startCkptLsn == null) {
                    preStartMapLNCount++;
                } else {
                    postStartMapLNCount++;
                }
            } else if (currentEntryTypeNum == 
                       LogEntryType.LOG_MAPLN_TRANSACTIONAL.getTypeNum()) {
                if (startCkptLsn == null) {
                    preStartMapLNTxnCount++;
                } else {
                    postStartMapLNTxnCount++;
                }
            }

        }
    }
}
