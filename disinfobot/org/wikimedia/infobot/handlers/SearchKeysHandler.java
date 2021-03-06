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
 * $Id: SearchKeysHandler.java,v 1.1 2004/11/22 18:27:58 kate Exp $
 */

package org.wikimedia.infobot.handlers;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.wikimedia.infobot.Infobot;
import org.wikimedia.infobot.User;
import org.wikimedia.infobot.irc.ServerMessage;

public class SearchKeysHandler extends Handler {
	public boolean execute(ServerMessage m, User u, String command) throws IOException {
		Pattern p = Pattern.compile("^searchkeys +(.*) *$");
		Matcher mat = p.matcher(command);

		if (!mat.find())
			return false;

		if (!checkPriv(m, u, 'S'))
			return true;
		
		String term = mat.group(1);
		String nick = m.prefix.getClient();
		List result;
		try {
			result = Infobot.ds.searchKeys(term);
		} catch (Exception e) {
			m.replyChannel("sorry " + nick +
					", your search term seems to be invalid: " + e.getMessage());
			return true;
		}
		if (result.size() == 0) {
			m.replyChannel("sorry " + nick + ", there were no matches.");
			return true;
		}
		String s = "";
		for (Iterator i = result.iterator(); i.hasNext();) {
			s += '"' + (String) i.next() + '"';
			if (i.hasNext())
				s += ", ";
		}
		if (s.length() > 400)
			s = s.substring(0, 400) + "...";
		m.replyChannel(nick + ", I found these matches: " + s);
		return true;
	}

}
