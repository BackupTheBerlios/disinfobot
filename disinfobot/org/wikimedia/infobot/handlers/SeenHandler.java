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
 * $Id: SeenHandler.java,v 1.1 2004/11/22 18:27:58 kate Exp $
 */
package org.wikimedia.infobot.handlers;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.wikimedia.infobot.Infobot;
import org.wikimedia.infobot.SeenEntry;
import org.wikimedia.infobot.User;
import org.wikimedia.infobot.irc.ServerMessage;

/**
 * @author Kate Turner
 *
 */
public class SeenHandler extends Handler {
	public boolean execute(ServerMessage m, User u, String command) throws IOException {
		Pattern p = Pattern.compile("^seen +(.*) *\\?? *$");
		Matcher mat = p.matcher(command);
		
		if (!mat.find())
			return false;

		if (!checkPriv(m, u, 'g'))
			return true;
		
		String nick = m.prefix.getClient();
		String target = mat.group(1);
		
		SeenEntry e = Infobot.seends.getItem(target);
		if (e == null) {
			m.replyChannel("sorry " + nick + ", I've never seen " + target + ".");
			return true;
		}
		
		String rep = nick + ", I last saw " + target + " ";
		switch (e.getType()) {
		case SeenEntry.T_NICK: {
			rep += "changing nick to " + e.getNick();
			break;
		}
		case SeenEntry.T_JOIN: {
			rep += "joining " + e.getChannel();
			break;
		}
		case SeenEntry.T_PART: {
			rep += "leaving " + e.getChannel();
			if (e.getData() != null)
				rep += " with the reason \"" + e.getData() + "\"";
			break;
		}
		case SeenEntry.T_PUBLIC: {
			rep += "saying \"" + e.getData() + "\" on " + e.getChannel();
			break;
		}
		case SeenEntry.T_QUIT: {
			rep += "quitting";
			if (e.getData() != null )
				rep += " with the reason \"" + e.getData() + "\"";
			break;
		}
		}
		SimpleDateFormat d = new SimpleDateFormat("d-MMM-yyyy HH:mm:ss Z");
		rep += " at " + d.format(e.getDate());
		m.replyChannel(rep);
		return true;
	}
}
