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
 * $Id: ForgetHandler.java,v 1.1 2004/11/22 18:27:58 kate Exp $
 */

package org.wikimedia.infobot.handlers;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.wikimedia.infobot.Factoid;
import org.wikimedia.infobot.Infobot;
import org.wikimedia.infobot.User;
import org.wikimedia.infobot.irc.ServerMessage;

public class ForgetHandler extends Handler {
	public boolean execute(ServerMessage m, User u, String command)
	throws IOException {
		Pattern p = Pattern.compile("^forget +(about +)?(.+) *$");
		Matcher mat = p.matcher(command);
		if (!mat.find())
			return false;
		
		Factoid f = Infobot.ds.getItem(mat.group(2));
		
		if (f == null || u.isAnonymous()
			|| !f.getAuthor().equals(u.getNickname()))
		{
			if (!checkPriv(m, u, 'd'))
				return true;
		}
		
		String nick = m.prefix.getClient();
		
		if (f == null) {
			Infobot.currentServer.privmsg(m.replyTo, "sorry, " + nick +
					", I don't know anything about " + mat.group(2));
			return true;
		}

		Infobot.ds.eraseItem(mat.group(2));
		Infobot.currentServer.privmsg(m.replyTo, "ok, " + nick);
		Infobot.logMsg("Forgot [" + mat.group(2) + "] (" + nick + ")");
		++Infobot.numAdditions;
		return true;
	}
}
