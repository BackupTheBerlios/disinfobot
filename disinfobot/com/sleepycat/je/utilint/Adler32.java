/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: Adler32.java,v 1.1 2004/11/22 18:27:58 kate Exp $
*/

package com.sleepycat.je.utilint;

import java.util.zip.Checksum;

/**
 * Adler32 checksum implementation.
 *
 * This class is used rather than the native java.util.zip.Adler32 class
 * because we have seen a JIT problem when calling the Adler32 class using
 * the Server JVM on Linux and Solaris.  Specifically, we suspect this may
 * be Bug Parade number 4965907.  See SR [#9376].
 *
 * The Adler32 checksum is discussed in RFC1950.  The sample implementation
 * from this RFC is shown below:
 *
 * <pre>
 *    #define BASE 65521  largest prime smaller than 65536
 *    unsigned long update_adler32(unsigned long adler,
 *       unsigned char *buf, int len)
 *    {
 *      unsigned long s1 = adler & 0xffff;
 *      unsigned long s2 = (adler >> 16) & 0xffff;
 *      int n;
 *
 *      for (n = 0; n < len; n++) {
 *        s1 = (s1 + buf[n]) % BASE;
 *        s2 = (s2 + s1)     % BASE;
 *      }
 *      return (s2 << 16) + s1;
 *    }
 *
 *    unsigned long adler32(unsigned char *buf, int len)
 *    {
 *      return update_adler32(1L, buf, len);
 *    }
 * </pre>
 *
 * The NMAX optimization is so that we don't have to do modulo calculations
 * on every iteration.  NMAX is the max number of additions to make
 * before you have to perform the modulo calculation.
 */
public class Adler32 implements Checksum {
    private long adler = 1;

    /*
     * BASE is the largest prime number smaller than 65536
     * NMAX is the largest n such that 255n(n+1)/2 + (n+1)(BASE-1) <= 2^32-1
     */
    private static final int BASE = 65521;
    private static final int NMAX = 5552;

    /**
     * Update current Adler-32 checksum given the specified byte.
     */
    public void update(int b) {
        long s1 = adler & 0xffff;
        long s2 = (adler >> 16) & 0xffff;
        s1 = (s1 + (b & 0xff)) % BASE;
        s2 = (s1 + s2) % BASE;
        adler = (s2 << 16) | s1;
    }

    /**
     * Update current Adler-32 checksum given the specified byte array.
     */
    public void update(byte[] b, int off, int len) {
        long s1 = adler & 0xffff;
        long s2 = (adler >> 16) & 0xffff;

        while (len > 0) {
            int k = len < NMAX ? len : NMAX;
            len -= k;

	    /* This does not benefit from loop unrolling. */
            while (k-- > 0) {
                s1 += (b[off++] & 0xff);
                s2 += s1;
            }

            s1 %= BASE;
            s2 %= BASE;
        }
        adler = (s2 << 16) | s1;
    }

    /**
     * Reset Adler-32 checksum to initial value.
     */
    public void reset() {
        adler = 1;
    }

    /**
     * Returns current checksum value.
     */
    public long getValue() {
        return adler;
    }
}
