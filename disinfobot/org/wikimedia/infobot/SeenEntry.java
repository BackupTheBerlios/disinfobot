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
 * $Id: SeenEntry.java,v 1.1 2004/11/22 18:27:57 kate Exp $
 */
package org.wikimedia.infobot;

import java.io.Serializable;
import java.util.Date;

/**
 * @author Kate Turner
 *
 */
public class SeenEntry implements Serializable {
	public static final int	T_PUBLIC = 1,
							T_JOIN = 2,
							T_PART = 3,
							T_QUIT = 4,
							T_NICK = 5;
	
	private final String nick, channel, data;
	private final Date date;
	private final int type;
	
	public SeenEntry(int type, String nick, String channel, String data) {
		this.type = type;
		this.nick = nick;
		this.channel = channel;
		this.data = data;
		this.date = new Date();
	}
	/**
	 * @return Returns the channel.
	 */
	public String getChannel() {
		return channel;
	}
	/**
	 * @return Returns the data.
	 */
	public String getData() {
		return data;
	}
	/**
	 * @return Returns the date.
	 */
	public Date getDate() {
		return date;
	}
	/**
	 * @return Returns the nick.
	 */
	public String getNick() {
		return nick;
	}
	/**
	 * @return Returns the type.
	 */
	public int getType() {
		return type;
	}
}
