/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: LastFileReader.java,v 1.1 2004/11/22 18:27:58 kate Exp $
*/

package com.sleepycat.je.log;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.Tracer;

/**
 * LastFileReader traverses the last log file, doing checksums and
 * looking for the end of the log. Different log types can be
 * registered with it and it will remember the last occurrence of
 * targetted entry types.
 */
public class LastFileReader extends FileReader {
    // Log entry types to track
    private Set trackableEntries;

    private long nextUnprovenOffset;
    private long lastValidOffset;

    // Last lsn seen for tracked types. Key = LogEntryType, data is
    // the offset (Long)
    private Map lastOffsetSeen;

    /**
     * This file reader is always positioned at the last file.
     */
    public LastFileReader(EnvironmentImpl env,
                          int readBufferSize)
        throws IOException, DatabaseException {

        super(env, readBufferSize, true,  null,
              new Long(0), null, null);

        trackableEntries = new HashSet();
        lastOffsetSeen = new HashMap();

        lastValidOffset = 0;
        nextUnprovenOffset = 0; 
    }

    /**
     * Override so that we always start at the last file.
     */
    protected void initStartingPosition(DbLsn endOfFileLsn,
					Long ignoreSingleFileNum)
        throws IOException, DatabaseException {

        eof = false;

        /*
         * Start at what seems like the last file. If it doesn't
         * exist, we're done
         */
        Long lastNum = fileManager.getLastFileNum();
        FileHandle fileHandle = null;
        readBufferFileEnd = 0;

        while ((fileHandle == null) && !eof) {
            if (lastNum == null) {
                eof = true;
            } else {
                try {
                    readBufferFileNum = lastNum.longValue();
                    fileHandle = fileManager.getFileHandle(readBufferFileNum);

                    /*
                     * Check the size of this file. If it opened successfully
                     * but only held a header, backup to the next "last" file
                     */
                    if (fileHandle.getFile().length() ==
                        FileManager.firstLogEntryOffset()) {
                        lastNum = fileManager.getFollowingFileNum
			    (lastNum.longValue(), false);
                        fileHandle.release();
                        fileHandle = null;
                    }
                } catch (DatabaseException e) {
                    lastNum = attemptToMoveBadFile(e);
                    fileHandle = null;
                } finally {
                    if (fileHandle != null) {
                        fileHandle.release();

                    }
                }
            }
        } 

        nextEntryOffset = 0;
    }

    /**
     * Something is wrong with this file. If there is no data in
     * this file (the header is <= the file header size) then move
     * this last file aside and search the next "last" file. If the
     * last file does have data in it, throw an exception back to the
     * application, since we're not sure what to do now.
     */
    private Long attemptToMoveBadFile(DatabaseException origException)
        throws DatabaseException, IOException {

        String fileName = fileManager.getFullFileNames(readBufferFileNum)[0];
        File problemFile = new File(fileName);
        Long lastNum = null;

        if (problemFile.length() <= FileManager.firstLogEntryOffset()) {
            fileManager.clear(); // close all existing files
            // move this file aside
            lastNum = fileManager.getFollowingFileNum(readBufferFileNum,
                                                      false);
            fileManager.renameFile(readBufferFileNum, 
                                   FileManager.BAD_SUFFIX);

        } else {
            // There's data in this file, throw up to the app
            throw origException;
        }
        return lastNum;
    }

    public void setEndOfFile() 
        throws IOException, DatabaseException  {

        fileManager.truncateLog(readBufferFileNum, nextUnprovenOffset);
    }

    /**
     * @return the lsn to be used for the next log entry.
     */
    public DbLsn getEndOfLog() {
        return new DbLsn(readBufferFileNum, nextUnprovenOffset);
    }

    public DbLsn getLastValidLsn() {
        return new DbLsn(readBufferFileNum, lastValidOffset);
    }

    public long getPrevOffset() {
        return lastValidOffset;
    }

    /**
     * Tell the reader that we are interested in these kind of entries.
     */
    public void setTargetType(LogEntryType type) {
        trackableEntries.add(type);
    }

    /**
     * @return the last lsn seen in the log for this kind of entry, or null.
     */
    public DbLsn getLastSeen(LogEntryType type) {
        Long typeNumber =(Long)lastOffsetSeen.get(type);
        if (typeNumber != null) {
            return new DbLsn(readBufferFileNum, typeNumber.longValue());
        }
        else {
            return null;
        }
    }

    /**
     * Validate the checksum on each entry, see if we should remember the lsn
     * of this entry.
     */
    protected boolean processEntry(ByteBuffer entryBuffer) {

        // Skip over the data, we're not doing anything with it
        entryBuffer.position(entryBuffer.position() + currentEntrySize);

        // If we're supposed to remember this lsn, record it 
        LogEntryType logType = new LogEntryType(currentEntryTypeNum,
                                                    currentEntryTypeVersion);
        if (trackableEntries.contains(logType)) {
            lastOffsetSeen.put(logType, new Long(currentEntryOffset));
        }

        return true;
    }

    /**
     * readNextEntry will stop at a bad entry.
     * @return true if an element has been read.
     */
    public boolean readNextEntry()
        throws DatabaseException, IOException {

        boolean foundEntry = false;

        nextUnprovenOffset = nextEntryOffset;
        try {

            /*
             * At this point, 
             *  currentEntryOffset is the entry we just read.
             *  nextEntryOffset is the entry we're about to read.
             *  currentEntryPrevOffset is 2 entries ago.
             * Note that readNextEntry() moves all the offset pointers up.
             */
            foundEntry = super.readNextEntry();

            /*
             * Note that initStartingPosition() makes sure that the
             * file header entry is valid.  So by the time we get to
             * this method, we know we're at a file with a valid file
             * header entry.
             */
            lastValidOffset = currentEntryOffset;
        } catch (DbChecksumException e) {
            Tracer.trace(Level.INFO,
                         env, "Found checksum exception while searching " +
                         " for end of log. Last valid entry is at " +
                         new DbLsn(readBufferFileNum, lastValidOffset) +
                         " Bad entry is at " +
                         new DbLsn(readBufferFileNum, currentEntryOffset));
        }
        return foundEntry;
    }
}
