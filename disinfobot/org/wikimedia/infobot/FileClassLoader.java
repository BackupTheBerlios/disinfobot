/*
 * Copyright 2004 Kate Turner
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy 
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights 
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell 
 * copies of the Software, and to permit persons to whom the Software is 
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in 
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR 
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE 
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 * 
 * $Id: FileClassLoader.java,v 1.1 2004/11/22 18:27:57 kate Exp $
 */

package org.wikimedia.infobot;

import java.io.File;
import java.io.FileInputStream;

/**
 * @author Kate Turner
 *
 */
public class FileClassLoader extends ClassLoader {
	public Class findClass(String name) throws ClassNotFoundException {
    	byte[] b = loadClassData(name);
        return defineClass(name, b, 0, b.length);
    }

    private byte[] loadClassData(String name) throws ClassNotFoundException {
    	String file = name.replaceAll("\\.", "/") + ".class";
    	FileInputStream fis;
    	try {
    		fis = new FileInputStream(new File(file));
        	int avail = fis.available();
        	byte[] ret = new byte[avail];
        	fis.read(ret);
        	return ret;
    	} catch (Exception e) {
    		ClassNotFoundException j = new ClassNotFoundException("Class not found: " + name);
    		Infobot.logStackTrace(j);
    		throw j;
    	}
    }
}
