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
 * $Id: GoogleHandler.java,v 1.1 2004/11/22 18:27:58 kate Exp $
 */

package org.wikimedia.infobot.handlers;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.wikimedia.infobot.Infobot;
import org.wikimedia.infobot.User;
import org.wikimedia.infobot.irc.ServerMessage;


import com.google.soap.search.*;

/**
 * Handles the 'google' command to search google.
 * 
 * @author Kate Turner
 *
 */
public class GoogleHandler extends Handler {
	public boolean execute(ServerMessage m, User u, String command) throws IOException {
		Pattern p = Pattern.compile("^google(for +)? +(.*) *\\?? *$");
		Matcher mat = p.matcher(command);
		
		if (!mat.find())
			return false;

		if (!checkPriv(m, u, 'g'))
			return true;
		
		String nick = m.prefix.getClient();

		if (!u.hasFlag('g')) {
			m.replyChannel("sorry " + nick + ", I don't think I should let you do that...");
			return true;
		}
		
		String target = nick;
		String search = mat.group(2);

		GoogleSearch gs = new GoogleSearch();
		gs.setKey(Infobot.cfg.getGoogleKey());
		gs.setQueryString(search);
		String result = "";
		String total;
		boolean isExact;
		try {
			GoogleSearchResult res = gs.doSearch();
			GoogleSearchResultElement[] results = res.getResultElements();
			if (results.length == 0) {
				result = "sorry " + nick + ", I didn't find any results. ";
				if (res.getSearchTips() != null)
					result += res.getSearchTips();
				m.replyChannel(result);
				return true;
			}
			result += results[0].getURL();
			String title = results[0].getTitle();
			if (title != null)
				result += " -- " + title;
			String snippet = results[0].getSnippet();
			if (snippet != null)
				result += " (" + snippet + ")";
			//result = result.replaceAll("</?b>", "\002");
			result = result.replaceAll("(</?b>|<br>)", "");
			total = NumberFormat.getInstance().format(res.getEstimatedTotalResultsCount());
			isExact = res.getEstimateIsExact();
		} catch (Exception e) {
			m.replyChannel("sorry " + nick + ", an error occured: " 
					+ e.toString().replaceAll("\n", ", "));
			return true;
		}
		m.replyChannel(nick + ", in a total of " + (!isExact ? "about " : "") + total
				+ " results, I found this: " + result);
		return true;
	}
}
