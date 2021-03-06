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
 * $Id: WelcomeHandler.java,v 1.1 2004/11/22 18:27:59 kate Exp $
 */

package org.wikimedia.infobot.irchandlers;

import java.io.IOException;
import java.util.Iterator;

import org.wikimedia.infobot.Infobot;
import org.wikimedia.infobot.irc.IRCConnection;
import org.wikimedia.infobot.irc.ServerMessage;


/**
 * @author Kate Turner
 *
 */
public class WelcomeHandler extends IRCHandler {

	/**
	 * @throws NoSuchMethodException
	 * @throws SecurityException
	 * 
	 */
	public static void install() throws SecurityException, NoSuchMethodException {
		IRCHandler.addIRCHandler("001", WelcomeHandler.class);
	}

	public WelcomeHandler(ServerMessage msg, IRCConnection server) {
		super(msg, server);
	}
	/* (non-Javadoc)
	 * @see infobot.IRCHandler#execute(infobot.ServerConnection, infobot.ServerMessage)
	 */
	public void execute(IRCConnection server, ServerMessage msg) throws IOException {
		for (Iterator i = Infobot.cfg.getChannels().iterator(); i.hasNext();) {
			server.joinChannel((String) i.next());
		}
	}

}
