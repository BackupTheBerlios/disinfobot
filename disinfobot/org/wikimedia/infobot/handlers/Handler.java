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
 * $Id: Handler.java,v 1.2 2004/12/21 11:12:02 kate Exp $
 */

package org.wikimedia.infobot.handlers;

import java.io.IOException;

import org.wikimedia.infobot.User;
import org.wikimedia.infobot.irc.ServerMessage;

public abstract class Handler {

	abstract public boolean execute(ServerMessage m, User u, String command) throws IOException;

	public String getDescription() {
		return "No description available";
	}
	
	public boolean checkPriv(ServerMessage m, User u, char priv) throws IOException {
		if (!u.hasFlag(priv)) {
			if (!u.isAnonymous() || (u.getFlags().length() > 0))
				m.replyChannel("sorry " + m.prefix.getClient() 
						+ ", I don't think I should let you do that...");
			return false;
		}
		return true;
	}
	
	public boolean getMatchesEverything() {
		return false;
	}
	public boolean getNoAddress() {
		return false;
	}
}
