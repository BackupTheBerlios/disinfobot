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
 * $Id: User.java,v 1.1 2004/11/22 18:27:57 kate Exp $
 */

package org.wikimedia.infobot;

import java.util.HashSet;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class User {
	static	HashSet		ds;
	
	public static void addUser(String nickname, String pfx, String flg) {
		if (ds == null)
			ds = new HashSet();
		
		ds.add(new User(nickname, pfx, flg));
	}
	
	public static void clearUsers() {
		if (ds == null)
			return;
		
		ds.clear();
	}
	
	public static User findByHost(String prefix) {
		if (ds == null)
			return new User();
		
		for (Iterator i = ds.iterator(); i.hasNext();) {
			User u = (User) i.next();
			Pattern p = Pattern.compile(u.prefix);
			Matcher m = p.matcher(prefix);
			if (m.find())
				return u;
		}
		return new User(); // anonymous user
	}
	
	protected String	nickname;
	/**
	 * @return Returns the nickname.
	 */
	public String getNickname() {
		return nickname;
	}
	
	protected String	prefix;
	protected String	flags;
	private boolean isAnonymous;
	
	protected User(String nickname, String host, String flags) {
		this.nickname = nickname;
		this.prefix = host;
		this.flags = flags;
		this.isAnonymous = false;
	}

	protected User() {
		this.isAnonymous = true;
		this.nickname = "<anonymous user>";
		this.flags = Infobot.cfg.getDefaultprivs();
	}
	
	public String getFlags() {
		return flags;
	}
	
	public boolean hasFlag(char c) {
		return flags.indexOf(c) != -1;
	}
	
	/**
	 * @return Returns whether this is an anonymous user.
	 */
	public boolean isAnonymous() {
		return isAnonymous;
	}
}
