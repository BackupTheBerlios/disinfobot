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
 * $Id: SeenDataStore.java,v 1.1 2004/11/22 18:27:57 kate Exp $
 */

package org.wikimedia.infobot;

import java.io.IOException;

import com.sleepycat.je.DatabaseException;

/**
 * @author Kate Turner
 *
 */
public class SeenDataStore extends DataStore {
	/**
	 * @param dbname
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws DatabaseException
	 */
	public SeenDataStore() throws IOException,
			ClassNotFoundException, DatabaseException {
		super("seen");
	}
	
	public void storeItem(String key, SeenEntry value) {
		storeItem(key, value, SeenEntry.class);
	}
	
	public SeenEntry getItem(String nick) {
		return (SeenEntry) getItem(nick, SeenEntry.class);
	}
}
