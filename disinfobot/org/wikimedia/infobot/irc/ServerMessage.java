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
 * $Id: ServerMessage.java,v 1.2 2004/12/20 07:28:26 kate Exp $
 */

package org.wikimedia.infobot.irc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.wikimedia.infobot.Prefix;

public class ServerMessage {
	public static final int SRC_SERVER = 1, SRC_CLIENT = 2;
	public int		source;
	public static final int T_PRIVMSG = 1, T_NOTICE = 2, T_PING = 3,
		T_CTCP = 4;
	public int				type;
		
	public String			text;
	public Prefix			prefix;
		
	public String			command;
	public List				arguments;
		
	public String			replyTo;
	private IRCConnection	server;
	public String			target;
	
	public boolean 			prv;
	
	public ServerMessage(IRCConnection server, String text) {
		this.text = text;
		this.server = server;
		parseMessage(text);
	}

	protected void parseMessage(String text) {
		this.text = new String(text);
		// prefix?
		if (text.charAt(0) == ':') {
			StringTokenizer t = new StringTokenizer(text, " ");
			String prefixtxt = t.nextToken().substring(1);
			text = text.substring(prefixtxt.length() + 1);
			prefix = new Prefix(prefixtxt);
		} else {
			prefix = new Prefix();
			source = SRC_SERVER;
		}
		
		List tokens = splitTokens(text);
		command = (String) tokens.get(0);
		tokens.remove(0);
		arguments = tokens;
		
		if (command.equals("PRIVMSG"))
			type = T_PRIVMSG;
		else if (command.equals("NOTICE"))
			type = T_NOTICE;
		else if (command.equals("PING"))
			type = T_PING;
		
		if (type == T_PRIVMSG || type == T_NOTICE) {
			target = (String) arguments.get(0);
			if (target.charAt(0) == '#' || target.charAt(0) == '+') {
				prv = false;
				replyTo = target;
			} else {
				replyTo = prefix.getClient();
				prv = true;
			}
		}

		if (type == T_PRIVMSG) {
			String s = (String) arguments.get(1);
			if (s.charAt(0) == '\001' && s.charAt(s.length() - 1) == '\001')
				type = T_CTCP;
		}
	}

	public static List splitTokens(String s) {
		List tokens = new ArrayList();
		String lastarg = null;
		int lastargidx = s.indexOf(" :");
		
		if (lastargidx != -1) {
			lastarg = s.substring(lastargidx + 2);
			s = s.substring(0, lastargidx);
		}
		
		StringTokenizer st = new StringTokenizer(s, " ");
		while (st.hasMoreTokens())
			tokens.add(st.nextToken());
	
		if (lastarg != null)
			tokens.add(lastarg);
		
		return tokens;
	}

	public void replySender(String message) throws IOException {
		server.privmsg(prefix.getClient(), message);
	}
	
	public void replyChannel(String message) throws IOException {
		server.privmsg(replyTo, message);
	}
	
	public void ctcpReply(String type, String args) throws IOException {
		String str = type + (args!=null ? " " + args : "");
		server.notice(prefix.getClient(), '\001' + str + '\001');
	}
	
	public String toString() {
		return text;
	}
}
