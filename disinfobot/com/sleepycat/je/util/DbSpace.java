/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: DbSpace.java,v 1.1 2004/11/22 18:27:58 kate Exp $
*/

package com.sleepycat.je.util;

import java.io.File;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.SortedMap;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.JEVersion;
import com.sleepycat.je.cleaner.FileSummary;
import com.sleepycat.je.cleaner.UtilizationProfile;

public class DbSpace {

    private static final String USAGE =
	"usage: java " + DbSpace.class.getName() + "\n" +
        "       -h <dir> # environment home directory\n" +
        "       [-q]     # quiet, print grand totals only\n" +
        "       [-u]     # sort by utilization\n" +
        "       [-V]     # print JE version number";

    public static void main(String argv[])
	throws DatabaseException {

	DbSpace space = new DbSpace();
	space.parseArgs(argv);

	try {
	    space.print(System.out);
	    System.exit(0);
	} catch (Throwable T) {
	    if (space.quiet) {
		System.exit(1);
	    } else {
		T.printStackTrace(System.err);
	    }
	} finally {
	    space.env.close();
	}
    }

    private File envHome = null;
    private Environment env;
    private boolean quiet = false;
    private boolean sorted = false;

    private DbSpace() {
    }

    public DbSpace(Environment env,
		   boolean quiet,
                   boolean sorted) {
	this.env = env;
	this.quiet = quiet;
	this.sorted = sorted;
    }

    private void printUsage(String msg) {
        if (msg != null) {
            System.err.println(msg);
        }
	System.err.println(USAGE);
	System.exit(-1);
    }

    private void parseArgs(String argv[]) {

	int argc = 0;
	int nArgs = argv.length;
        
        if (nArgs == 0) {
	    printUsage(null);
            System.exit(0);
        }

	while (argc < nArgs) {
	    String thisArg = argv[argc++];
	    if (thisArg.equals("-q")) {
		quiet = true;
            } else if (thisArg.equals("-u")) {
		sorted = true;
	    } else if (thisArg.equals("-V")) {
		System.out.println(JEVersion.CURRENT_VERSION);
		System.exit(0);
	    } else if (thisArg.equals("-h")) {
		if (argc < nArgs) {
		    envHome = new File(argv[argc++]);
		} else {
		    printUsage("-h requires an argument");
		}
	    }
	}

	if (envHome == null) {
	    printUsage("-h is a required argument");
	}
    }

    private void openEnv()
	throws DatabaseException {

	if (env == null) {
            EnvironmentConfig envConfig = new EnvironmentConfig();
            envConfig.setReadOnly(true);
	    env = new Environment(envHome, envConfig);
	}
    }

    public void print(PrintStream out)
	throws DatabaseException {

	openEnv();

        UtilizationProfile profile = DbInternal.envGetEnvironmentImpl(env)
                                               .getUtilizationProfile();
        SortedMap map = profile.getFileSummaryMap(false);
        int fileIndex = 0;

        Summary totals = new Summary();
        Summary[] summaries = null;
        if (!quiet) {
            summaries = new Summary[map.size()];
        }

        for (Iterator i = map.keySet().iterator(); i.hasNext();) {
            Long fileNum = (Long) i.next();
            Summary summary = new Summary(fileNum,
                                          (FileSummary) map.get(fileNum),
                                          fileIndex, profile);
            if (summaries != null) {
                summaries[fileIndex] = summary;
            }
            totals.add(summary);
            fileIndex += 1;
        }

        out.println(Summary.HEADER);

        if (summaries != null) {
            if (sorted) {
                Arrays.sort(summaries);
            }
            for (int i = 0; i < summaries.length; i += 1) {
                summaries[i].print(out);
            }
        }

        totals.print(out);
    }

    private static class Summary implements Comparable {

        static final String HEADER = "  File    Size (KB)  % Used\n" +
                                     "--------  ---------  ------";
                                   // 12345678  123456789     123
                                   //         12         12345
                                   // TOTALS:

        Long fileNum;
        long totalSize;
        long obsoleteSize;

        Summary() {}

        Summary(Long fileNum, FileSummary summary, int fileIndex,
                UtilizationProfile profile)
            throws DatabaseException {

            this.fileNum = fileNum;
            totalSize = summary.totalSize;
            obsoleteSize = summary.getObsoleteSize(fileIndex, profile);
        }

        public int compareTo(Object other) {
            Summary o = (Summary) other;
            return utilization() - o.utilization();
        }

        void add(Summary o) {
            totalSize += o.totalSize;
            obsoleteSize += o.obsoleteSize;
        }

        void print(PrintStream out) {
            if (fileNum != null) {
                pad(out, Long.toHexString(fileNum.longValue()), 8, '0');
            } else {
                out.print(" TOTALS ");
            }
            int kb = (int) (totalSize / 1024);
            int util = utilization();
            out.print("  ");
            pad(out, Integer.toString(kb), 9, ' ');
            out.print("     ");
            pad(out, Integer.toString(util), 3, ' ');
            out.println();
        }

        int utilization() {
            return UtilizationProfile.utilization(obsoleteSize, totalSize);
        }

        private void pad(PrintStream out, String val, int digits,
                           char padChar) {
            int padSize = digits - val.length();
            for (int i = 0; i < padSize; i += 1) {
                out.print(padChar);
            }
            out.print(val);
        }
    }
}
