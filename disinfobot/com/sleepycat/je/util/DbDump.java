/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: DbDump.java,v 1.1 2004/11/22 18:27:58 kate Exp $
*/

package com.sleepycat.je.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.JEVersion;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.utilint.Tracer;

public class DbDump {
    private static final int VERSION = 3;

    private File envHome = null;
    protected Environment env;
    private String dbName = null;
    private boolean formatUsingPrintable;
    private boolean dupSort;
    private String outputFileName = null;
    private PrintStream outputFile = null;

    private String usageString =
	"usage: DbDump [-f output-file] [-l] [-p] [-V]\n" +
	"              [-s database] -h dbEnvHome\n";

    private DbDump() {
    }

    public DbDump(Environment env,
		  String dbName,
		  PrintStream outputFile,
		  boolean formatUsingPrintable) {
	this.env = env;
	this.dbName = dbName;
	this.outputFile = outputFile;
	this.formatUsingPrintable = formatUsingPrintable;
    }

    static public void main(String argv[])
	throws DatabaseException, IOException {

	DbDump dumper = new DbDump();
	boolean listDbs = dumper.parseArgs(argv);

	if (listDbs) {
	    dumper.listDbs();
	    System.exit(0);
	}

	try {
	    dumper.dump();
	} catch (Throwable T) {
	    T.printStackTrace();
	} finally {
	    dumper.env.close();
            if (dumper.outputFile != System.out) {
                dumper.outputFile.close();
            }
	}
    }

    private void listDbs()
	throws DatabaseException {

	openEnv();

	List dbNames = env.getDatabaseNames();
	Iterator iter = dbNames.iterator();
	while (iter.hasNext()) {
	    String name = (String) iter.next();
	    System.out.println(name);
	}
    }

    private void printUsage(String msg) {
	System.err.println(msg);
	System.err.println(usageString);
	System.exit(-1);
    }

    private boolean parseArgs(String argv[])
	throws IOException {

	int argc = 0;
	int nArgs = argv.length;
	boolean listDbs = false;
	while (argc < nArgs) {
	    String thisArg = argv[argc++];
	    if (thisArg.equals("-p")) {
		formatUsingPrintable = true;
	    } else if (thisArg.equals("-V")) {
		System.out.println(JEVersion.CURRENT_VERSION);
		System.exit(0);
	    } else if (thisArg.equals("-l")) {
		listDbs = true;
	    } else if (thisArg.equals("-f")) {
		if (argc < nArgs) {
		    outputFileName = argv[argc++];
		} else {
		    printUsage("-f requires an argument");
		}
	    } else if (thisArg.equals("-h")) {
		if (argc < nArgs) {
		    String envDir = argv[argc++];
                    envHome = new File(envDir);
		} else {
		    printUsage("-h requires an argument");
		}
	    } else if (thisArg.equals("-s")) {
		if (argc < nArgs) {
		    dbName = argv[argc++];
		} else {
		    printUsage("-s requires an argument");
		}
	    }
	}

	if (envHome == null) {
	    printUsage("-h is a required argument");
	}
	if (!listDbs) {
	    if (dbName == null) {
		printUsage("Must supply a database name if -l not supplied.");
	    }
	}

	if (outputFileName == null) {
	    outputFile = System.out;
	} else {
	    outputFile = new PrintStream(new FileOutputStream(outputFileName));
	}

	return listDbs;
    }

    /*
     * Begin DbDump API.  From here on there should be no calls to printUsage,
     * System.xxx.print, or System.exit.
     */
    private void openEnv()
	throws DatabaseException {

	if (env == null) {
            EnvironmentConfig envConfiguration = new EnvironmentConfig();
            envConfiguration.setReadOnly(true);
	    env = new Environment(envHome, envConfiguration);
	}
    }

    public void dump()
	throws IOException, DatabaseException {

	openEnv();

	Tracer.trace(Level.INFO, DbInternal.envGetEnvironmentImpl(env),
		     "DbDump.dump of " + dbName + " starting");

	DatabaseEntry foundKey = new DatabaseEntry();
	DatabaseEntry foundData = new DatabaseEntry();

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setReadOnly(true);
        DbInternal.setUseExistingConfig(dbConfig, true);
        Database db = env.openDatabase(null, dbName, dbConfig);
	dupSort = db.getConfig().getSortedDuplicates();

	printHeader(outputFile, formatUsingPrintable);

	Cursor cursor = db.openCursor(null, null);
	while (cursor.getNext(foundKey, foundData, LockMode.DEFAULT) ==
               OperationStatus.SUCCESS) {
	    dumpOne(outputFile, foundKey, formatUsingPrintable);
	    dumpOne(outputFile, foundData, formatUsingPrintable);
	}
	cursor.close();
	db.close();
	outputFile.println("DATA=END");

	Tracer.trace(Level.INFO, DbInternal.envGetEnvironmentImpl(env),
		     "DbDump.dump of " + dbName + " ending");
    }

    private void printHeader(PrintStream o, boolean formatUsingPrintable) {
	o.println("VERSION=" + VERSION);
	if (formatUsingPrintable) {
	    o.println("format=print");
	} else {
	    o.println("format=bytevalue");
	}
	o.println("type=btree");
	o.println("dupsort=" + (dupSort ? "1" : "0"));
	o.println("HEADER=END");
    }

    static private final String printableChars =
	" !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
	"[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~";

    private void dumpOne(PrintStream o, DatabaseEntry dbt,
			 boolean formatUsingPrintable) {
	byte[] ba = dbt.getData();
        o.print(' ');
	for (int i = 0; i < ba.length; i++) {
	    int b = ba[i] & 0xff;
	    if (formatUsingPrintable) {
		if (isPrint(b)) {
		    if (b == 0134) {  /* backslash */
			o.print('\\');
		    }
		    o.print(printableChars.charAt(b - 32));
		} else {
		    o.print('\\');
		    String hex = Integer.toHexString(b);
		    if (b < 16) {
			o.print('0');
		    }
		    o.print(hex);
		}
	    } else {
		String hex = Integer.toHexString(b);
		if (b < 16) {
		    o.print('0');
		}
		o.print(hex);
	    }
	}
	o.println("");
    }

    private boolean isPrint(int b) {
	return (b < 0177) && (037 < b);
    }
}
