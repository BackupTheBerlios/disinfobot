/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: SecondaryDatabase.java,v 1.1 2004/11/22 18:27:58 kate Exp $
*/

package com.sleepycat.je;

import java.util.logging.Level;

import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.GetMode;
import com.sleepycat.je.dbi.PutMode;
import com.sleepycat.je.dbi.CursorImpl.SearchMode;
import com.sleepycat.je.txn.Locker;
import com.sleepycat.je.utilint.Tracer;

/**
 * Javadoc for this public class is generated via
 * the doc templates in the doc_src directory.
 */
public class SecondaryDatabase extends Database {

    private Database primaryDb;
    private SecondaryConfig secondaryConfig;
    private SecondaryTrigger secondaryTrigger;
    private ForeignKeyTrigger foreignKeyTrigger;

    /**
     * Creates a secondary database but does not open or fully initialize it.
     */
    SecondaryDatabase(Environment env, SecondaryConfig secConfig,
                      Database primaryDatabase)
        throws DatabaseException {

        super(env);
        DatabaseUtil.checkForNullParam(primaryDatabase, "primaryDatabase");
        primaryDatabase.checkRequiredDbState(OPEN, "Can't use as primary:");
        if (primaryDatabase.configuration.getSortedDuplicates()) {
            throw new IllegalArgumentException
                ("Duplicates must not be allowed for a primary database: " +
                 primaryDatabase.getDatabaseName());
        }
        if (env.getEnvironmentImpl() !=
                primaryDatabase.getEnvironment().getEnvironmentImpl()) {
            throw new IllegalArgumentException
                ("Primary and secondary databases must be in the same" +
                 " environment.");
        }
        if (!primaryDatabase.configuration.getReadOnly() &&
            secConfig.getKeyCreator() == null) {
            throw new NullPointerException
                ("secConfig and secConfig.getKeyCreator() may be null" +
                 " only if the primary database is read-only");
        }
        if (secConfig.getForeignKeyDatabase() != null &&
            secConfig.getForeignKeyDeleteAction() ==
                         ForeignKeyDeleteAction.NULLIFY &&
            secConfig.getForeignKeyNullifier() == null) {
            throw new IllegalArgumentException
                ("ForeignKeyNullifier must be non-null when " +
                 " ForeignKeyDeleteAction is NULLIFY.");
        }
        primaryDb = primaryDatabase;
        secondaryTrigger = new SecondaryTrigger(this);
        if (secConfig.getForeignKeyDatabase() != null) {
            foreignKeyTrigger = new ForeignKeyTrigger(this);
        }
    }

    /**
     * Create a database, called by Environment
     */
    void initNew(Environment env,
                 Locker locker,
                 String databaseName,
                 DatabaseConfig dbConfig)
        throws DatabaseException {

        super.initNew(env, locker, databaseName, dbConfig);
        init(locker);
    }

    /**
     * Open a database, called by Environment
     */
    void initExisting(Environment env,
                      Locker locker,
                      DatabaseImpl database,
                      DatabaseConfig dbConfig)
        throws DatabaseException {

        super.initExisting(env, locker, database, dbConfig);
        init(locker);
    }

    /**
     * Adds secondary to primary's list, and populates the secondary if needed.
     */
    private void init(Locker locker)
        throws DatabaseException {

        Tracer.trace(Level.FINEST,
                     envHandle.getEnvironmentImpl(),
                     "SecondaryDatabase open: name=" + getDatabaseName() +
                     " primary=" + primaryDb.getDatabaseName());

        secondaryConfig = (SecondaryConfig) configuration;

        /* Insert foreign key triggers at the front of the list and append
         * secondary triggers at the end, so that ForeignKeyDeleteAction.ABORT
         * is applied before deleting the secondary keys. */

        primaryDb.addTrigger(secondaryTrigger, false);

        Database foreignDb = secondaryConfig.getForeignKeyDatabase();
        if (foreignDb != null) {
            foreignDb.addTrigger(foreignKeyTrigger, true);
        }

        /* Populate secondary if requested and secondary is empty. */
        if (secondaryConfig.getAllowPopulate()) {
            Cursor secCursor = null;
            Cursor priCursor = null;
            try {
                secCursor = new Cursor(this, locker, null);
                DatabaseEntry key = new DatabaseEntry();
                DatabaseEntry data = new DatabaseEntry();
                OperationStatus status = secCursor.position(key, data,
                                                            LockMode.DEFAULT,
                                                            true);
                if (status == OperationStatus.NOTFOUND) {
                    /* Is empty, so populate */
                    priCursor = new Cursor(primaryDb, locker, null);
                    status = priCursor.position(key, data, LockMode.DEFAULT,
                                                true);
                    while (status == OperationStatus.SUCCESS) {
                        updateSecondary(locker, secCursor, key, null, data);
                        status = priCursor.retrieveNext(key, data,
                                                        LockMode.DEFAULT,
                                                        GetMode.NEXT);
                    }
                }
            } finally {
                if (secCursor != null) {
                    secCursor.close();
                }
                if (priCursor != null) {
                    priCursor.close();
                }
            }
        }
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public synchronized void close()
        throws DatabaseException {

        if (primaryDb != null && secondaryTrigger != null) {
            primaryDb.removeTrigger(secondaryTrigger);
        }
        Database foreignDb = secondaryConfig.getForeignKeyDatabase();
        if (foreignDb != null && foreignKeyTrigger != null) {
            foreignDb.removeTrigger(foreignKeyTrigger);
        }
        super.close();
    }

    /**
     * Should be called by the secondaryTrigger while holding a write lock on
     * the trigger list.
     */
    void clearPrimary() {
        primaryDb = null;
        secondaryTrigger  = null;
    }

    /**
     * Should be called by the foreignKeyTrigger while holding a write lock on
     * the trigger list.
     */
    void clearForeignKeyTrigger() {
        foreignKeyTrigger = null;
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public Database getPrimaryDatabase()
        throws DatabaseException {

        return primaryDb;
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public SecondaryConfig getSecondaryConfig()
        throws DatabaseException {

        return (SecondaryConfig) getConfig();
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public SecondaryCursor openSecondaryCursor(Transaction txn,
                                               CursorConfig cursorConfig)
        throws DatabaseException {

        return (SecondaryCursor) openCursor(txn, cursorConfig);
    }
 
    /**
     * Overrides Database method.
     */
    Cursor newDbcInstance(Transaction txn,
                          CursorConfig cursorConfig)
        throws DatabaseException {

        return new SecondaryCursor(this, txn, cursorConfig, secondaryConfig);
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public OperationStatus delete(Transaction txn,
                                  DatabaseEntry key)
        throws DatabaseException {

        checkEnv();
        DatabaseUtil.checkForNullDbt(key, "key", true);
        checkRequiredDbState(OPEN, "Can't call SecondaryDatabase.delete:");
        trace(Level.FINEST, "SecondaryDatabase.delete", txn,
              key, null, null);

        Locker locker = null;
        Cursor cursor = null;

        OperationStatus commitStatus = OperationStatus.NOTFOUND;
        try {
            locker =
                DatabaseUtil.getWritableLocker(envHandle,
                                               txn, isTransactional());

            /* Read the primary key (the data of a secondary). */
            cursor = new Cursor(this, locker, null);
            DatabaseEntry pKey = new DatabaseEntry();
            OperationStatus searchStatus =
                cursor.search(key, pKey, LockMode.RMW, SearchMode.SET);

            /* Delete the primary and all secondaries (including this one). */
            if (searchStatus == OperationStatus.SUCCESS) {
                commitStatus = primaryDb.deleteInternal(locker, pKey);
            } 
            return commitStatus;
        } finally {
            if (cursor != null) {
                cursor.close();
            }
            if (locker != null) {
                locker.operationEnd(commitStatus);
            }
        }
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public OperationStatus get(Transaction txn,
                               DatabaseEntry key,
                               DatabaseEntry data,
                               LockMode lockMode)
        throws DatabaseException {

        return get(txn, key, new DatabaseEntry(), data, lockMode);
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public OperationStatus get(Transaction txn,
                               DatabaseEntry key,
                               DatabaseEntry pKey,
                               DatabaseEntry data,
                               LockMode lockMode)
        throws DatabaseException {

        checkEnv();
        DatabaseUtil.checkForNullDbt(key, "key", true);
        DatabaseUtil.checkForNullDbt(pKey, "pKey", false);
        DatabaseUtil.checkForNullDbt(data, "data", false);
        checkRequiredDbState(OPEN, "Can't call SecondaryDatabase.get:");
        trace(Level.FINEST, "SecondaryDatabase.get", txn, key, null, lockMode);

        SecondaryCursor cursor = null;
        try {
	    cursor = new SecondaryCursor(this, txn, CursorConfig.DEFAULT,
                                         secondaryConfig);
            return cursor.search(key, pKey, data, lockMode, SearchMode.SET);
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public OperationStatus getSearchBoth(Transaction txn,
                                         DatabaseEntry key,
                                         DatabaseEntry data,
                                         LockMode lockMode)
        throws DatabaseException {

        throw notAllowedException();
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public OperationStatus getSearchBoth(Transaction txn,
                                         DatabaseEntry key,
                                         DatabaseEntry pKey,
                                         DatabaseEntry data,
                                         LockMode lockMode)
        throws DatabaseException {

        checkEnv();
        DatabaseUtil.checkForNullDbt(key, "key", true);
        DatabaseUtil.checkForNullDbt(pKey, "pKey", true);
        DatabaseUtil.checkForNullDbt(data, "data", true);
        checkRequiredDbState(OPEN,
                             "Can't call SecondaryDatabase.getSearchBoth:");
        trace(Level.FINEST, "SecondaryDatabase.getSearchBoth", txn, key, data,
              lockMode);

        SecondaryCursor cursor = null;
        try {
	    cursor = new SecondaryCursor(this, txn, CursorConfig.DEFAULT,
                                         secondaryConfig);
            return cursor.search(key, pKey, data, lockMode, SearchMode.BOTH);
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public OperationStatus put(Transaction txn,
                               DatabaseEntry key,
                               DatabaseEntry data)
        throws DatabaseException {

        throw notAllowedException();
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public OperationStatus putNoOverwrite(Transaction txn,
                                          DatabaseEntry key,
                                          DatabaseEntry data)
        throws DatabaseException {

        throw notAllowedException();
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public OperationStatus putNoDupData(Transaction txn,
                                        DatabaseEntry key,
                                        DatabaseEntry data)
        throws DatabaseException {

        throw notAllowedException();
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public JoinCursor join(Cursor[] cursors, JoinConfig config)
        throws DatabaseException {

        throw notAllowedException();
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public int truncate(Transaction txn, boolean countRecords)
        throws DatabaseException {

        throw notAllowedException();
    }
    
    /**
     * Updates a single secondary when a put() or delete() is performed on
     * the primary.
     *
     * @param txn the user transaction.
     *
     * @param cursor secondary cursor to use, or null if this method should
     * open and close a cursor if one is needed.
     *
     * @param priKey the primary key.
     *
     * @param oldData the primary data before the change, or null if the record
     * did not previously exist.
     *
     * @param newData the primary data after the change, or null if the record
     * has been deleted.
     */
    void updateSecondary(Locker locker,
                         Cursor cursor,
                         DatabaseEntry priKey,
                         DatabaseEntry oldData,
                         DatabaseEntry newData)
        throws DatabaseException {

        SecondaryKeyCreator keyCreator = secondaryConfig.getKeyCreator();

        /* Get old and new secondary keys. */
        DatabaseEntry oldSecKey = null;
        if (oldData != null) {
            oldSecKey = new DatabaseEntry();
            if (!keyCreator.createSecondaryKey(this, priKey, oldData,
                                               oldSecKey)) {
                oldSecKey = null;
            }
        }
        DatabaseEntry newSecKey = null;
        if (newData != null) {
            newSecKey = new DatabaseEntry();
            if (!keyCreator.createSecondaryKey(this, priKey, newData,
                                               newSecKey)) {
                newSecKey = null;
            }
        }

        /* Update secondary if old and new keys are unequal. */
        if ((oldSecKey != null && !oldSecKey.dataEquals(newSecKey)) ||
            (newSecKey != null && !newSecKey.dataEquals(oldSecKey))) {

            boolean localCursor = (cursor == null);
            if (localCursor) {
                cursor = new Cursor(this, locker, null);
            }
            try {
                /* Insert new secondary key. */
                if (newSecKey != null) {
                    Database foreignDb =
                        secondaryConfig.getForeignKeyDatabase();
                    if (foreignDb != null) {
                        Cursor foreignCursor = null;
                        try {
                            foreignCursor = new Cursor(foreignDb, locker,
                                                       null);
                            DatabaseEntry tmpData = new DatabaseEntry();
                            OperationStatus status =
                                foreignCursor.search(newSecKey, tmpData,
                                                     LockMode.DEFAULT,
                                                     SearchMode.SET);
                            if (status != OperationStatus.SUCCESS) {
                                throw new DatabaseException
                                    ("Secondary " + getDatabaseName() +
                                     " foreign key not allowed: it is not" +
                                     " present in the foreign database");
                            }
                        } finally {
                            if (foreignCursor != null) {
                                foreignCursor.close();
                            }
                        }
                    }
                    OperationStatus status;
                    if (getConfig().getSortedDuplicates()) {
                        status = cursor.putInternal(newSecKey, priKey,
                                                    PutMode.NODUP);
                    } else {
                        status = cursor.putInternal(newSecKey, priKey,
                                                    PutMode.NOOVERWRITE);
                    }
                    if (status != OperationStatus.SUCCESS) {
                        throw new DatabaseException
                            ("Could not insert secondary key in " +
                             getDatabaseName() + ' ' + status);
                    }
                }

                /* Delete old secondary key. */
                if (oldSecKey != null) {
                    OperationStatus status =
                        cursor.search(oldSecKey, priKey,
                                      LockMode.RMW,
                                      SearchMode.BOTH);
                    if (status == OperationStatus.SUCCESS) {
                        cursor.deleteInternal();
                    } else {
                        throw new DatabaseException
                            ("Secondary " + getDatabaseName() +
                            " is corrupt: the primary record contains a key" +
                            " that is not present in the secondary");
                    }
                }
            } finally {
                if (localCursor && cursor != null) {
                    cursor.close();
                }
            }
        }
    }

    /**
     * Called by the ForeignKeyTrigger when a record in the foreign database is
     * deleted.
     *
     * @param secKey is the primary key of the foreign database, which is the
     * secondary key (ordinary key) of this secondary database.
     */
    void onForeignKeyDelete(Locker locker, DatabaseEntry secKey)
        throws DatabaseException {

        ForeignKeyDeleteAction deleteAction =
            secondaryConfig.getForeignKeyDeleteAction();

        /* Use RMW if we're going to be deleting the secondary records. */
        LockMode lockMode = (deleteAction == ForeignKeyDeleteAction.ABORT)
                            ? LockMode.DEFAULT : LockMode.RMW;

        /*
         * Use the deleted foreign primary key to read the data of this
         * database, which is the associated primary's key.
         */
        DatabaseEntry priKey = new DatabaseEntry();
        Cursor cursor = null;
        OperationStatus status;
        try {
	    cursor = new Cursor(this, locker, null);
            status = cursor.search(secKey, priKey, lockMode,
                                   SearchMode.SET);
            while (status == OperationStatus.SUCCESS) {

                if (deleteAction == ForeignKeyDeleteAction.ABORT) {

                    /*
                     * ABORT - throw an exception to cause the user to abort
                     * the transaction.
                     */
                    throw new DatabaseException
                        ("Secondary " + getDatabaseName() +
                         " refers to a foreign key that has been deleted" +
                         " (ForeignKeyDeleteAction.ABORT)");

                } else if (deleteAction == ForeignKeyDeleteAction.CASCADE) {

                    /*
                     * CASCADE - delete the associated primary record.
                     */
                    Cursor priCursor = null;
                    try {
                        DatabaseEntry data = new DatabaseEntry();
                        priCursor = new Cursor(primaryDb, locker, null);
                        status = priCursor.search(priKey, data, LockMode.RMW,
                                                  SearchMode.SET);
                        if (status == OperationStatus.SUCCESS) {
                            priCursor.delete();
                        } else {
                            throw secondaryCorruptException();
                        }
                    } finally {
                        if (priCursor != null) {
                            priCursor.close();
                        }
                    }

                } else if (deleteAction == ForeignKeyDeleteAction.NULLIFY) {

                    /*
                     * NULLIFY - set the secondary key to null in the
                     * associated primary record.
                     */
                    Cursor priCursor = null;
                    try {
                        DatabaseEntry data = new DatabaseEntry();
                        priCursor = new Cursor(primaryDb, locker, null);
                        status = priCursor.search(priKey, data, LockMode.RMW,
                                                  SearchMode.SET);
                        if (status == OperationStatus.SUCCESS) {
                            ForeignKeyNullifier nullifier =
                                secondaryConfig.getForeignKeyNullifier();
                            if (nullifier.nullifyForeignKey(this, data)) {
                                priCursor.putCurrent(data);
                            }
                        } else {
                            throw secondaryCorruptException();
                        }
                    } finally {
                        if (priCursor != null) {
                            priCursor.close();
                        }
                    }
                } else {
                    /* Should never occur. */
                    throw new IllegalStateException();
                }

                status = cursor.retrieveNext(secKey, priKey, LockMode.DEFAULT,
                                             GetMode.NEXT_DUP);
            }
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
    }

    DatabaseException secondaryCorruptException()
        throws DatabaseException {

        throw new DatabaseException
            ("Secondary " + getDatabaseName() + " is corrupt: it refers" +
             " to a missing key in the primary database");
    }

    static UnsupportedOperationException notAllowedException() {

        throw new UnsupportedOperationException
            ("Operation not allowed on a secondary");
    }
}
