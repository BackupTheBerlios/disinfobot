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
 * $Id: PrivmsgHandler.java,v 1.3 2004/12/21 11:12:02 kate Exp $
 */

package org.wikimedia.infobot.irchandlers;

import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.wikimedia.infobot.Infobot;
import org.wikimedia.infobot.SeenEntry;
import org.wikimedia.infobot.User;
import org.wikimedia.infobot.handlers.Handler;
import org.wikimedia.infobot.irc.IRCConnection;
import org.wikimedia.infobot.irc.ServerMessage;


/**
 * @author Kate Turner
 *
 */
public class PrivmsgHandler extends IRCHandler {
	/**
	 * Construct a new handler and installs it in the handler table.
	 * @throws NoSuchMethodException
	 * @throws SecurityException
	 */
	public static void install() throws SecurityException, NoSuchMethodException {
		IRCHandler.addIRCHandler("PRIVMSG", PrivmsgHandler.class);
	}

	public PrivmsgHandler(ServerMessage msg, IRCConnection server) {
		super(msg, server);
	}

	/**
	 * Handles the IRC PRIVMSG command;
	 * @throws IOException
	 * @see org.wikimedia.infobot.irchandlers.IRCHandler#execute(infobot.ServerConnection, infobot.ServerMessage)
	 */
	public void execute(IRCConnection server, ServerMessage msg) throws IOException {
		if (!msg.prv) {
			SeenEntry s = new SeenEntry(SeenEntry.T_PUBLIC, null,
					msg.target, (String) msg.arguments.get(1));
			Infobot.seends.storeItem(msg.prefix.getClient(), s);
		}
		
		boolean addressed = false;
		String nick = server.getCurrentNickname();
		String matchme = "^" + (msg.prv?"":(nick + "[,:> ]+"))+"(.*?)$";
		Pattern p = Pattern.compile(matchme);
		Matcher mat = p.matcher((String) msg.arguments.get(1));

		if (mat.find())
			addressed = true;
		
		String command = addressed ? mat.group(1) : (String)msg.arguments.get(1);
		
		try {
			handleCommand(msg, command, addressed);
		} catch (Exception e) {
			if (addressed)
				msg.replyChannel( 
						"sorry " + msg.prefix.getClient() +
						", there seems to have been an internal error: " + e);
			Infobot.logStackTrace(e);
		}
		return;
	}
	
	protected void handleCommand(ServerMessage m, String command, boolean addressed)
	throws IOException {
		User u = User.findByHost(m.prefix.getPrefix());
		Pattern p;
		Matcher mat;
		String nick = m.prefix.getClient();
		
		if (addressed) {
			p = Pattern.compile("^listhandlers$");
			mat = p.matcher(command);
			if (mat.find()) {
				if (!u.hasFlag('a')) {
					if (!u.isAnonymous())
						m.replyChannel("sorry " + nick +
						", I don't think I should let you do that.");
					return;
				}
				for (Iterator i = Infobot.handlers.iterator(); i.hasNext(); ) {
					Handler h = (Handler) i.next();
					m.replyChannel("[" +
							h.getClass().getName() + "] " + h.getDescription());
				}
				return;
			}
			p = Pattern.compile("^instantiate +(.+) *$");
			mat = p.matcher(command);
			if (mat.find()) {
				if (!u.hasFlag('a')) {
					if (!u.isAnonymous())
						m.replyChannel("sorry " + nick +
						", I don't think I should let you do that.");
					return;
				}
				try {
					Infobot.loadHandler(mat.group(1));
				} catch (Exception e) {
					m.replyChannel("sorry " + nick +
							", an error seems to have occured: " + e);
					return;
				}
				m.replyChannel("ok, " + nick);
				return;
			}
			
			p = Pattern.compile("^deinstantiate +(.+) *$");
			mat = p.matcher(command);
			if (mat.find()) {
				if (!u.hasFlag('a')) {
					if (!u.isAnonymous())
						m.replyChannel("sorry " + nick +
						", I don't think I should let you do that.");
					return;
				}
				try {
					Infobot.unloadHandler(mat.group(1));
				} catch (Exception e) {
					m.replyChannel("sorry " + nick + ", an error seems to have occured: " 
							+ e);
					return;
				}
				m.replyChannel("ok, " + nick);
				return;
			}
		}
		
		for (Iterator i = Infobot.handlers.iterator(); i.hasNext();) {
			Handler h = (Handler) i.next();
			if (addressed || h.getNoAddress())
				if (h.execute(m, u, command))
					return;
		}

		if (!addressed)
			return;
		
		if (!(u.getFlags().length() == 0))
			m.replyChannel("sorry " + nick + ", I'm not sure what you mean...");
	}
}
