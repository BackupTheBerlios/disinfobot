/*-
* See the file LICENSE for redistribution information.
*
* Copyright (c) 2002-2004
*      Sleepycat Software.  All rights reserved.
*
* $Id: JEStat.java,v 1.1 2004/11/22 18:27:58 kate Exp $
*/

package com.sleepycat.je;

import java.lang.reflect.Field;

/**
 * Provide a reflective toString();
 */
class JEStat {
    
    /** 
     * Print all data members by reflection so we don't have to maintain
     * a toString implementation as we change the stats.
     */
    public String toString() {
        try {
            Class c = getClass();
            Field [] f = c.getDeclaredFields();
        
            StringBuffer sb = new StringBuffer();
            for (int i = 0; i < f.length; i++) {
                sb.append(f[i].getName()).append("=").append(f[i].get(this));
                sb.append('\n');
            }
            return sb.toString();
        } catch (IllegalAccessException e) {
            return e.getMessage();
        }
    }
}
