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
 * $Id: AddEntryHandler.java,v 1.1 2004/11/22 18:27:58 kate Exp $
 */

package org.wikimedia.infobot.handlers;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.wikimedia.infobot.Factoid;
import org.wikimedia.infobot.Infobot;
import org.wikimedia.infobot.User;
import org.wikimedia.infobot.irc.ServerMessage;

public class AddEntryHandler extends Handler {
	public boolean execute(ServerMessage m, User u, String command) throws IOException {
		Pattern p = Pattern.compile("^ *(.+?) +is +(.+) *$");
		Matcher mat = p.matcher(command);

		if (!mat.find())
			return false;
		
		if (!checkPriv(m, u, 'm'))
			return true;
		
		String nick = m.prefix.getClient();
		
		String name = mat.group(1);
		if (!Factoid.isSanitoryName(name)) {
			m.replyChannel("sorry " + nick + ", " + Factoid.sanitiseName(name) +
					" is not a sanitory name");
			return true;
		}
		// setting key
		if (Infobot.ds.keyExists(name)) {
			m.replyChannel("I already know about " + mat.group(1) + ", " + nick);
			return true;
		}
		String text = mat.group(2).trim();
		if (text.length() == 0 || text.equals("<reply>")) {
			m.replyChannel(nick + ", what's the point of an empty factoid?");
			return true;
		}
		Factoid f = new Factoid();
		f.setText(mat.group(2));
		f.setAuthor(u.getNickname());
		Infobot.ds.storeItem(name, f);
		Infobot.currentServer.privmsg(m.replyTo, "ok, " + nick);
		Infobot.logMsg("Learnt [" + name + "] => [" + mat.group(2) + "]"
				+ " (" + u.getNickname() + ")");
		++Infobot.numAdditions;
		return true;
	}
}
