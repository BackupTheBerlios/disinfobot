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
 * $Id: NormalLookupHandler.java,v 1.2 2004/11/22 20:16:16 kate Exp $
 */

package org.wikimedia.infobot.handlers;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.wikimedia.infobot.Factoid;
import org.wikimedia.infobot.Infobot;
import org.wikimedia.infobot.User;
import org.wikimedia.infobot.irc.ServerMessage;

public class NormalLookupHandler extends Handler {
	public boolean getMatchesEverything() {
		return true;
	}
	
	public boolean execute(ServerMessage m, User u, String command) throws IOException {
		Pattern p = Pattern.compile(
				"^(tell +([^ ]+) +(privately +)?about +)?(raw +)?(.+?) *(\\?|\\.)? *$");
		Matcher mat = p.matcher(command);
		
		if (!mat.find())
			return false;

		if (!checkPriv(m, u, 'q'))
			return true;
		
		String nick = m.prefix.getClient();
		String target = nick;
		
		boolean indirect = false;
		if (mat.group(1) != null && mat.group(1).length() > 0) {
			if (!mat.group(2).equals("me"))
				target = mat.group(2);
			indirect = true;
		}
		String name = mat.group(5);
		if (name.equals("me"))
			name = nick;
		Factoid f = Infobot.ds.getItem(name);
		if (f == null) {
			Infobot.currentServer.privmsg(m.replyTo, "no idea about " +
					name + ", " + nick);
			return true;
		}
		++Infobot.numQueries;
		f.incQueries();
		Infobot.ds.storeItem(name, f);
		String val;
		String reply = m.replyTo;
		boolean raw = (mat.group(4) != null);
		if (raw) {
			val = f.getText();
			m.replyChannel(nick + ", raw text is: \"" + Factoid.sanitiseName(val) + "\".");
			return true;
		}
		if (mat.group(3) != null && u.hasFlag('p'))
			reply = target;
		if (mat.group(4) != null)
			val = f.getText();
		else
			val = f.getTextLinks();
		if (val.startsWith("<reply>")) {
			if (indirect)
				Infobot.currentServer.privmsg(reply, target + ": " + val.substring(7));
			else
				Infobot.currentServer.privmsg(reply, val.substring(7));
		} else {
			String replystr = name + " is ";
			if (target.equals(name))
				replystr = "you are ";
			Infobot.currentServer.privmsg(reply, target + ", " + replystr + val);
		}
		return true;
	}
}
