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
 * $Id: TinyurlHandler.java,v 1.1 2004/12/21 11:12:02 kate Exp $
 */

package org.wikimedia.infobot.handlers;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.wikimedia.infobot.Infobot;
import org.wikimedia.infobot.User;
import org.wikimedia.infobot.irc.ServerMessage;

/**
 * @author Kate Turner
 *
 */
public class TinyurlHandler extends Handler {
	public boolean getNoAddress() {
		return true;
	}
	public boolean execute(ServerMessage m, User u, String command) throws IOException {
		long then = System.currentTimeMillis();
		Pattern p = Pattern.compile("(http://[^ ]{50,})");
		Matcher mat = p.matcher(command);
		
		if (!mat.find())
			return false;

		String nick = m.prefix.getClient();
		String target = nick;
		
		String urlstr = "http://tinyurl.com/create.php?url="+
			mat.group(1);
		URL url = new URL(urlstr);
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		conn.setUseCaches(true);
		conn.setInstanceFollowRedirects(false);
		conn.setRequestProperty("User-Agent", 
				"Wikipedia-Java-Infobot/" + Infobot.version + " (contact: kate.turner@gmail.com)");
		conn.connect();
		int i = conn.getResponseCode();
		if (i != 200) {
			return false;
		}
		long now = System.currentTimeMillis();
		double time = (now - (double)then) / 1000.0;
		p = Pattern.compile("\\[<a href=\"(http://tinyurl.com/[^\"]+)\" target=\"_blank\">");
		DataInputStream dis = new DataInputStream(new BufferedInputStream(
				conn.getInputStream()));
		String tinyurl = null;
		String line;
		while ((line = dis.readLine()) != null) {
			mat = p.matcher(line);
			if (mat.find()) {
				tinyurl = mat.group(1);
				break;
			}
		}
		dis.close();
		m.replyChannel(tinyurl);
		return true;
	}

}
