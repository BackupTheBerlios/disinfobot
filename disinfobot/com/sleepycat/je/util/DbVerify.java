/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: DbVerify.java,v 1.1 2004/11/22 18:27:58 kate Exp $
*/

package com.sleepycat.je.util;

import java.io.File;
import java.io.PrintStream;
import java.util.logging.Level;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DatabaseStats;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.JEVersion;
import com.sleepycat.je.VerifyConfig;
import com.sleepycat.je.utilint.Tracer;

public class DbVerify {
    protected File envHome = null;
    protected Environment env;
    protected String dbName = null;
    protected boolean quiet = false;

    private String usageString =
	"usage: DbVerify [-q] [-V]\n" +
	"                -s database -h dbEnvHome [-v progressInterval]\n";

    private int progressInterval = 0;

    static public void main(String argv[])
	throws DatabaseException {

	DbVerify verifier = new DbVerify();
	verifier.parseArgs(argv);

	try {
	    boolean ret = verifier.verify(System.err);
	    System.exit(ret ? 0 : -1);
	} catch (Throwable T) {
	    if (verifier.quiet) {
		System.exit(1);
	    } else {
		T.printStackTrace(System.err);
	    }
	} finally {
	    verifier.env.close();
	}
    }

    protected DbVerify() {
    }

    public DbVerify(Environment env,
		    String dbName,
		    boolean quiet) {
	this.env = env;
	this.dbName = dbName;
	this.quiet = quiet;
    }

    protected void printUsage(String msg) {
	System.err.println(msg);
	System.err.println(usageString);
	System.exit(-1);
    }

    protected void parseArgs(String argv[]) {

	int argc = 0;
	int nArgs = argv.length;
	while (argc < nArgs) {
	    String thisArg = argv[argc++];
	    if (thisArg.equals("-q")) {
		quiet = true;
	    } else if (thisArg.equals("-V")) {
		System.out.println(JEVersion.CURRENT_VERSION);
		System.exit(0);
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
	    } else if (thisArg.equals("-v")) {
		if (argc < nArgs) {
		    progressInterval = Integer.parseInt(argv[argc++]);
		    if (progressInterval <= 0) {
			printUsage("-v requires a positive argument");
		    }
		} else {
		    printUsage("-v requires an argument");
		}
	    }
	}

	if (envHome == null) {
	    printUsage("-h is a required argument");
	}

	if (dbName == null) {
	    printUsage("-s is a required argument");
	}
    }

    protected void openEnv()
	throws DatabaseException {

	if (env == null) {
            EnvironmentConfig envConfig = new EnvironmentConfig();
            envConfig.setReadOnly(true);
	    env = new Environment(envHome, envConfig);
	}
    }

    public boolean verify(PrintStream out)
	throws DatabaseException {

	boolean ret = true;
	try {
	    openEnv();

	    Tracer.trace(Level.INFO, DbInternal.envGetEnvironmentImpl(env),
			 "DbVerify.verify of " + dbName + " starting");

	    DatabaseConfig dbConfig = new DatabaseConfig();
	    dbConfig.setReadOnly(true);
	    dbConfig.setAllowCreate(false);
	    DbInternal.setUseExistingConfig(dbConfig, true);
	    Database db = env.openDatabase(null, dbName, dbConfig);

	    VerifyConfig verifyConfig = new VerifyConfig();
	    verifyConfig.setPrintInfo(true);
	    if (progressInterval > 0) {
		verifyConfig.setShowProgressInterval(progressInterval);
		verifyConfig.setShowProgressStream(out);
	    }
	    DatabaseStats stats = db.verify(verifyConfig);
	    out.println(stats);

	    db.close();
	    Tracer.trace(Level.INFO, DbInternal.envGetEnvironmentImpl(env),
			 "DbVerify.verify of " + dbName + " ending");
	} catch (DatabaseException DE) {
	    ret = false;
	    throw DE;
	}
	return ret;
    }
}
