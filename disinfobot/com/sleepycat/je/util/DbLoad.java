/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: DbLoad.java,v 1.1 2004/11/22 18:27:58 kate Exp $
*/

package com.sleepycat.je.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.logging.Level;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.JEVersion;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.utilint.Tracer;

public class DbLoad {
    private static final boolean DEBUG = false;

    protected Environment env;
    private boolean formatUsingPrintable;
    private String dbName;
    private BufferedReader reader;
    private boolean noOverwrite;
    private boolean textFileMode;
    private boolean dupSort;
    private boolean ignoreUnknownConfig;
    private boolean commandLine;

    static private String usageString =
	"usage: DbLoad [-f input-file] [-n] [-V] [-T] [-I]\n" +
	"              [-c name=value]\n" +
	"              [-s database] -h dbEnvHome\n";

    static public void main(String argv[])
	throws DatabaseException, IOException {

	DbLoad loader = parseArgs(argv);

        try {
            loader.load();
        } catch (Throwable e) {
            e.printStackTrace();
        }

	loader.env.close();
    }

    static private void printUsage(String msg) {
	System.err.println(msg);
	System.err.println(usageString);
	System.exit(-1);
    }

    static private DbLoad parseArgs(String argv[])
	throws IOException, DatabaseException {

	boolean noOverwrite = false;
	boolean textFileMode = false;
	boolean ignoreUnknownConfig = false;

	int argc = 0;
	int nArgs = argv.length;
	String inputFileName = null;
	File envHome = null;
	String dbName = null;
	DbLoad ret = new DbLoad();
        ret.setCommandLine(true);

	while (argc < nArgs) {
	    String thisArg = argv[argc++].trim();
	    if (thisArg.equals("-n")) {
		noOverwrite = true;
	    } else if (thisArg.equals("-T")) {
		textFileMode = true;
	    } else if (thisArg.equals("-I")) {
		ignoreUnknownConfig = true;
	    } else if (thisArg.equals("-V")) {
		System.out.println(JEVersion.CURRENT_VERSION);
		System.exit(0);
	    } else if (thisArg.equals("-f")) {
		if (argc < nArgs) {
		    inputFileName = argv[argc++];
		} else {
		    printUsage("-f requires an argument");
		}
	    } else if (thisArg.equals("-h")) {
		if (argc < nArgs) {
		    envHome = new File(argv[argc++]);
		} else {
		    printUsage("-h requires an argument");
		}
	    } else if (thisArg.equals("-s")) {
		if (argc < nArgs) {
		    dbName = argv[argc++];
		} else {
		    printUsage("-s requires an argument");
		}
	    } else if (thisArg.equals("-c")) {
		if (argc < nArgs) {
                    try {
                        ret.loadConfigLine(argv[argc++]);
                    } catch (IllegalArgumentException e) {
                        printUsage("-c: " + e.getMessage());
                    }
		} else {
		    printUsage("-c requires an argument");
		}
	    }
	}

	if (envHome == null) {
	    printUsage("-h is a required argument");
	}

	InputStream is;
	if (inputFileName == null) {
	    is = System.in;
	} else {
	    is = new FileInputStream(inputFileName);
	}
	BufferedReader reader = new BufferedReader(new InputStreamReader(is));

        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);
	Environment env = new Environment(envHome, envConfig);
	ret.setEnv(env);
	ret.setDbName(dbName);
	ret.setInputReader(reader);
	ret.setNoOverwrite(noOverwrite);
	ret.setTextFileMode(textFileMode);
	ret.setIgnoreUnknownConfig(ignoreUnknownConfig);
	return ret;
    }

    /*
     * Begin DbLoad API.  From here on there should be no calls to printUsage,
     * System.xxx.print, or System.exit.
     */

    public DbLoad() {
    }

    /**
     * If true, enables output of warning messages.  Command line behavior is
     * not available via the public API.
     */
    private void setCommandLine(boolean commandLine) {
        this.commandLine = commandLine;
    }

    public void setEnv(Environment env) {
	this.env = env;
    }

    public void setDbName(String dbName) {
	this.dbName = dbName;
    }

    public void setInputReader(BufferedReader reader) {
	this.reader = reader;
    }

    public void setNoOverwrite(boolean noOverwrite) {
	this.noOverwrite = noOverwrite;
    }

    public void setTextFileMode(boolean textFileMode) {
	this.textFileMode = textFileMode;
    }

    public void setIgnoreUnknownConfig(boolean ignoreUnknownConfigMode) {
	this.ignoreUnknownConfig = ignoreUnknownConfigMode;
    }

    public boolean load()
	throws IOException, DatabaseException {

	Tracer.trace(Level.INFO, DbInternal.envGetEnvironmentImpl(env),
		     "DbLoad.load of " + dbName + " starting");

        if (textFileMode) {
            formatUsingPrintable = true;
        } else {
            loadHeader();
        }

        if (dbName == null) {
            throw new IllegalArgumentException
                ("Must supply a database name if -l not supplied.");
        }

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setSortedDuplicates(dupSort);
        dbConfig.setAllowCreate(true);
        Database db = env.openDatabase(null, dbName, dbConfig);

        loadData(db);

        db.close();

        Tracer.trace(Level.INFO, DbInternal.envGetEnvironmentImpl(env),
                     "DbLoad.load of " + dbName + " ending.");

        return true;
    }

    private void loadConfigLine(String line)
	throws DatabaseException {

	int equalsIdx = line.indexOf('=');
	if (equalsIdx < 0) {
	    throw new IllegalArgumentException
                ("Invalid header parameter: " + line);
	}

	String keyword = line.substring(0, equalsIdx).trim().toLowerCase();
	String value = line.substring(equalsIdx + 1).trim();

	if (keyword.equals("version")) {
	    if (DEBUG) {
		System.out.println("Found version: " + line);
	    }
	    if (!value.equals("3")) {
		throw new IllegalArgumentException
                    ("Version " + value + " is not supported.");
	    }
	} else if (keyword.equals("format")) {
	    value = value.toLowerCase();
	    if (value.equals("print")) {
		formatUsingPrintable = true;
	    } else if (value.equals("bytevalue")) {
		formatUsingPrintable = false;
	    } else {
		throw new IllegalArgumentException
		    (value + " is an unknown value for the format keyword");
	    }
	    if (DEBUG) {
		System.out.println("Found format: " + formatUsingPrintable);
	    }
	} else if (keyword.equals("dupsort")) {
	    value = value.toLowerCase();
	    if (value.equals("true") ||
		value.equals("1")) {
		dupSort = true;
	    } else if (value.equals("false") ||
		       value.equals("0")) {
		dupSort = false;
	    } else {
		throw new IllegalArgumentException
		    (value + " is an unknown value for the dupsort keyword");
	    }
	    if (DEBUG) {
		System.out.println("Found dupsort: " + dupSort);
	    }
	} else if (keyword.equals("type")) {
	    value = value.toLowerCase();
	    if (!value.equals("btree")) {
		throw new IllegalArgumentException
                    (value + " is not a supported database type.");
	    }
	    if (DEBUG) {
		System.out.println("Found type: " + line);
	    }
	} else if (keyword.equals("database")) {
	    if (dbName == null) {
		dbName = value;
	    }
	    if (DEBUG) {
		System.out.println("DatabaseImpl: " + dbName);
	    }
	} else if (!ignoreUnknownConfig) {
	    throw new IllegalArgumentException
                ("'" + line + "' is not understood.");
	}
    }

    private void loadHeader()
	throws IOException, DatabaseException {

	if (DEBUG) {
	    System.out.println("loading header");
	}
	String line = reader.readLine();
	while (!line.equals("HEADER=END")) {
	    loadConfigLine(line);
	    line = reader.readLine();
	}
    }

    private void loadData(Database db)
	throws DatabaseException, IOException {

	String keyLine = reader.readLine();
	String dataLine = null;
	while (keyLine != null &&
	       !keyLine.equals("DATA=END")) {
	    dataLine = reader.readLine();
            if (dataLine == null) {
                throw new DatabaseException("No data to match key " +
                                            keyLine);
            }
	    byte[] keyBytes = loadLine(keyLine.trim());
	    byte[] dataBytes = loadLine(dataLine.trim());

	    DatabaseEntry key = new DatabaseEntry(keyBytes);
	    DatabaseEntry data = new DatabaseEntry(dataBytes);

	    if (DEBUG) {
		System.out.print(".");
	    }
	    if (noOverwrite) {
		if (db.putNoOverwrite(null, key, data) ==
		    OperationStatus.KEYEXIST) {
                    /* Calling println is OK only from command line. */
                    if (commandLine) {
                        System.err.println("Key exists: " + key);
                    }
		}
	    } else {
		db.put(null, key, data);
	    }
	    keyLine = reader.readLine();
	}
    }

    private byte[] loadLine(String line)
	throws DatabaseException {

	if (formatUsingPrintable) {
	    return readPrintableLine(line);
	}
	int nBytes = line.length() / 2;
	byte[] ret = new byte[nBytes];
	int charIdx = 0;
	for (int i = 0; i < nBytes; i++, charIdx += 2) {
	    int b2 = Character.digit(line.charAt(charIdx), 16);
	    b2 <<= 4;
	    b2 += Character.digit(line.charAt(charIdx + 1), 16);
	    ret[i] = (byte) b2;
	}
	return ret;
    }

    static private byte backSlashValue =
	(byte) (new Character('\\').charValue() & 0xff);

    private byte[] readPrintableLine(String line)
	throws DatabaseException {

	/* nBytes is the max number of bytes that this line could turn into. */
	int maxNBytes = line.length();
	byte[] ba = new byte[maxNBytes];
	int actualNBytes = 0;

	for (int charIdx = 0; charIdx < maxNBytes; charIdx++) {
	    char c = line.charAt(charIdx);
	    if (c == '\\') {
		if (++charIdx < maxNBytes) {
		    char c1 = line.charAt(charIdx);
		    if (c1 == '\\') {
			ba[actualNBytes++] = backSlashValue;
		    } else {
			if (++charIdx < maxNBytes) {
			    char c2 = line.charAt(charIdx);
			    int b = Character.digit(c1, 16);
			    b <<= 4;
			    b += Character.digit(c2, 16);
			    ba[actualNBytes++] = (byte) b;
			} else {
			    throw new DatabaseException("Corrupted file");
			}
		    }
		} else {
		    throw new DatabaseException("Corrupted file");
		}
	    } else {
		ba[actualNBytes++] = (byte) (c & 0xff);
	    }
	}

	if (maxNBytes == actualNBytes) {
	    return ba;
	} else {
	    byte[] ret = new byte[actualNBytes];
	    System.arraycopy(ba, 0, ret, 0, actualNBytes);
	    return ret;
	}
    }
}
