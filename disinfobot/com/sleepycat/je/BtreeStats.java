/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: BtreeStats.java,v 1.1 2004/11/22 18:27:58 kate Exp $
*/

package com.sleepycat.je;

/**
 */
public class BtreeStats extends DatabaseStats {

    /* Number of Bottom Internal Nodes in the database's btree. */
    private long binCount;

    /* Number of Duplicate Bottom Internal Nodes in the database's btree. */
    private long dbinCount;

    /* Number of deleted Leaf Nodes in the database's btree. */
    private long deletedLNCount;

    /* Number of duplicate Leaf Nodes in the database's btree. */
    private long dupCountLNCount;

    /* 
     * Number of Internal Nodes in database's btree.  BIN's are not included.
     */
    private long inCount;

    /* 
     * Number of Duplicate Internal Nodes in database's btree.  BIN's are not
     * included.
     */
    private long dinCount;

    /* Number of Leaf Nodes in the database's btree. */
    private long lnCount;

    /* Maximum depth of the in memory tree. */
    private int mainTreeMaxDepth;

    /* Maximum depth of the duplicate memory trees. */
    private int duplicateTreeMaxDepth;

    /* Histogram of INs by level. */
    private long[] insByLevel;

    /* Histogram of BINs by level. */
    private long[] binsByLevel;

    /* Histogram of DINs by level. */
    private long[] dinsByLevel;

    /* Histogram of DBINs by level. */
    private long[] dbinsByLevel;

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public long getBottomInternalNodeCount() {
        return binCount;
    }

    /**
     * @param val
     */
    public void setBottomInternalNodeCount(long val) {
        binCount = val;
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public long getDuplicateBottomInternalNodeCount() {
        return dbinCount;
    }

    /**
     * @param val
     */
    public void setDuplicateBottomInternalNodeCount(long val) {
        dbinCount = val;
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public long getDeletedLeafNodeCount() {
        return deletedLNCount;
    }

    /**
     * @param val
     */
    public void setDeletedLeafNodeCount(long val) {
        deletedLNCount = val;
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public long getDupCountLeafNodeCount() {
        return dupCountLNCount;
    }

    /**
     * @param val
     */
    public void setDupCountLeafNodeCount(long val) {
        dupCountLNCount = val;
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public long getInternalNodeCount() {
        return inCount;
    }

    /**
     * @param val
     */
    public void setInternalNodeCount(long val) {
        inCount = val;
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public long getDuplicateInternalNodeCount() {
        return dinCount;
    }

    /**
     * @param val
     */
    public void setDuplicateInternalNodeCount(long val) {
        dinCount = val;
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public long getLeafNodeCount() {
        return lnCount;
    }

    /**
     * @param val
     */
    public void setLeafNodeCount(long val) {
        lnCount = val;
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public int getMainTreeMaxDepth() {
        return mainTreeMaxDepth;
    }

    /**
     * @param val
     */
    public void setMainTreeMaxDepth(int val) {
        mainTreeMaxDepth = val;
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public int getDuplicateTreeMaxDepth() {
        return duplicateTreeMaxDepth;
    }

    /**
     * @param val
     */
    public void setDuplicateTreeMaxDepth(int val) {
        duplicateTreeMaxDepth = val;
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public long[] getINsByLevel() {
        return insByLevel;
    }

    /**
     * @param val
     */
    public void setINsByLevel(long[] insByLevel) {
	this.insByLevel = insByLevel;
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public long[] getBINsByLevel() {
        return binsByLevel;
    }

    /**
     * @param val
     */
    public void setBINsByLevel(long[] binsByLevel) {
	this.binsByLevel = binsByLevel;
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public long[] getDINsByLevel() {
        return dinsByLevel;
    }

    /**
     * @param val
     */
    public void setDINsByLevel(long[] dinsByLevel) {
	this.dinsByLevel = dinsByLevel;
    }

    /**
     * Javadoc for this public method is generated via
     * the doc templates in the doc_src directory.
     */
    public long[] getDBINsByLevel() {
        return dbinsByLevel;
    }

    /**
     * @param val
     */
    public void setDBINsByLevel(long[] dbinsByLevel) {
	this.dbinsByLevel = dbinsByLevel;
    }

    private void arrayToString(long[] arr, StringBuffer sb) {
	for (int i = 0; i < arr.length; i++) {
	    long count = arr[i];
	    if (count > 0) {
		sb.append("   <Item level=\"").append(i);
		sb.append("\" count=\"").append(count).append("\"/>\n");
	    }
	}
    }

    public String toString() {
	StringBuffer sb = new StringBuffer();
	sb.append("<BtreeStats\n");
	if (binCount > 0) {
	    sb.append(" <BottomInternalNodesByLevel total=\"");
	    sb.append(binCount).append("\">\n");
	    arrayToString(binsByLevel, sb);
	    sb.append(" </BottomInternalNodesByLevel>\n");
	}
	if (inCount > 0) {
	    sb.append(" <InternalNodesByLevel total=\"");
	    sb.append(inCount).append("\">\n");
	    arrayToString(insByLevel, sb);
	    sb.append(" </InternalNodesByLevel>\n");
	}
	if (dinCount > 0) {
	    sb.append(" <DuplicateInternalNodesByLevel total=\"");
	    sb.append(dinCount).append("\">\n");
	    arrayToString(dinsByLevel, sb);
	    sb.append(" </DuplicateInternalNodesByLevel>\n");
	}
	if (dbinCount > 0) {
	    sb.append(" <DuplicateBottomInternalNodesByLevel total=\"");
	    sb.append(dbinCount).append("\">\n");
	    arrayToString(dbinsByLevel, sb);
	    sb.append(" </DuplicateBottomInternalNodesByLevel>\n");
	}
	sb.append(" <LeafNodeCount=\"").append(lnCount).append("\"/>\n");
	sb.append(" <DeletedLeafNodeCount=\"").
	    append(deletedLNCount).append("\"/>\n");
	sb.append(" <DuplicateCountLeafNodeCount=\"").
	    append(dupCountLNCount).append("\"/>\n");
	sb.append(" <MainTreeMaxDepth=\"").
	    append(mainTreeMaxDepth).append("\"/>\n");
	sb.append(" <DuplicateTreeMaxDepth=\"").
	    append(duplicateTreeMaxDepth).append("\"/>\n");
	sb.append("</BtreeStats>\n");
	return sb.toString();
    }
}
