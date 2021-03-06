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
 * $Id: PartHandler.java,v 1.3 2004/11/30 02:11:11 kate Exp $
 */
package org.wikimedia.infobot.irchandlers;

import java.io.IOException;

import org.wikimedia.infobot.Infobot;
import org.wikimedia.infobot.SeenEntry;
import org.wikimedia.infobot.irc.IRCConnection;
import org.wikimedia.infobot.irc.ServerMessage;

/**
 * @author Kate Turner
 *
 */
public class PartHandler extends IRCHandler {
	/**
	 * @throws NoSuchMethodException
	 * @throws SecurityException
	 * 
	 */
	public static void install() throws SecurityException, NoSuchMethodException {
		IRCHandler.addIRCHandler("PART", PartHandler.class);
	}

	public PartHandler(ServerMessage msg, IRCConnection server) {
		super(msg, server);
	}
	
	public void execute(IRCConnection server, ServerMessage msg) throws IOException {
		String partmsg = (String) (msg.arguments.size() > 1 ? msg.arguments.get(1) : null);
		SeenEntry s = new SeenEntry(SeenEntry.T_PART, null,
				(String) msg.arguments.get(0), partmsg);
		Infobot.seends.storeItem(msg.prefix.getClient(), s);
	}
}
