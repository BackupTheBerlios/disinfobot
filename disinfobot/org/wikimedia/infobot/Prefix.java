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
 * $Id: Prefix.java,v 1.1 2004/11/22 18:27:57 kate Exp $
 */

package org.wikimedia.infobot;

/**
 *
 * A prefix represents a nick!user@host prefix from an IRC message
 * and extracts the individual components;
 *  
 * @author Kate Turner
 *
 */
public class Prefix {
	protected String	prefix;
	
	protected String	client;
	protected String	username;
	protected String	hostname;
	protected int		type;
	
	static public final int
		T_SERVER = 1, T_USER = 2;
	
	/**
	 * @return The nickname or server name of the client.
	 */
	public String getClient() {
		return client;
	}

	/**
	 * @return The username of the client, or <code>null</code>.
	 */
	public String getUsername() {
		return username;
	}
	
	/**
	 * @return The hostname of the client, or <code>null</code>.
	 */
	public String getHostname() {
		return hostname;
	}
	
	/**
	 * Returns the type (user or server) of the prefix.
	 * 
	 * @return <code>T_USER</code> for users, and <code>T_SERVER</code> for servers.
	 */
	public int getType() {
		return type;
	}
	
	/**
	 * Returns the whole prefix as a string.
	 */
	public String getPrefix() {
		return prefix;
	}
	
	/**
	 * Construct a prefix from a "nick!user@host" or "server.name" string;
	 * 
	 * @param pfx Prefix string.
	 */
	public Prefix (String pfx) {
		this.prefix = pfx;
		extractPrefix();
	}
	
	/**
	 * Construct a null prefix object.
	 *
	 */
	public Prefix () {
		this.client = "";
		this.prefix = "";
	}
	
	protected void extractPrefix() {
		int i = prefix.indexOf('@');
		if (i == -1) { // server, no host
			client = prefix;
			type = T_SERVER;
			return;
		}
		client = prefix.substring(0, i);
		int j = prefix.indexOf('!');
		client = prefix.substring(0, j);
		username = prefix.substring(j + 1, i);
		hostname = prefix.substring(i + 1);
		type = T_USER;
	}
}
