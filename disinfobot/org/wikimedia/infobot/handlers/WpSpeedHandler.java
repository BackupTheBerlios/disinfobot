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
 * $Id: WpSpeedHandler.java,v 1.1 2004/11/22 18:27:58 kate Exp $
 */

package org.wikimedia.infobot.handlers;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.wikimedia.infobot.Infobot;
import org.wikimedia.infobot.User;
import org.wikimedia.infobot.irc.ServerMessage;

/**
 * @author Kate Turner
 *
 */
public class WpSpeedHandler extends Handler {

	/* (non-Javadoc)
	 * @see infobot.Handler#execute(infobot.ServerMessage, infobot.User, java.lang.String)
	 */
	public boolean execute(ServerMessage m, User u, String command) throws IOException {
		long then = System.currentTimeMillis();
		Pattern p = Pattern.compile("^(wikipedia|wp) *status *\\?? *$");
		Matcher mat = p.matcher(command);
		
		if (!mat.find())
			return false;

		if (!checkPriv(m, u, 'q'))
			return true;
		
		String nick = m.prefix.getClient();
		String target = nick;
		
		String urlstr = "http://en.wikipedia.org/wiki/Main_Page";
		URL url = new URL(urlstr);
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		conn.setInstanceFollowRedirects(false);
		conn.setRequestProperty("User-Agent", 
				"Wikipedia-Java-Infobot/" + Infobot.version + " (contact: kate.turner@gmail.com)");
		conn.connect();
		int i = conn.getResponseCode();
		if (i != 200) {
			m.replyChannel(nick + ", I couldn't load the Wikipedia main page.  " 
					+ "The server returned an unexpected reply: "
					+ i + " " + conn.getResponseMessage());
			return true;
		}
		long now = System.currentTimeMillis();
		double time = (now - (double)then) / 1000.0;
		m.replyChannel(nick + ", I loaded the Wikipedia main page in " + time + " seconds.");
		return true;
	}

}
