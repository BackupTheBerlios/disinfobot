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
 * $Id: TimeHandler.java,v 1.1 2004/11/22 18:27:58 kate Exp $
 */

package org.wikimedia.infobot.handlers;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.wikimedia.infobot.User;
import org.wikimedia.infobot.irc.ServerMessage;

/**
 * @author Kate Turner
 *
 */
public class TimeHandler extends Handler {
	protected static String guessTimeZone(String place) {
		String[] zones = TimeZone.getAvailableIDs();
		place = place.replaceAll(" ", "_");
		for (int i = 0; i < zones.length; ++i) {
			if (zones[i].toLowerCase().equals(place.toLowerCase()))
				return zones[i];
		}
		for (int i = 0; i < zones.length; ++i) {
			if (zones[i].toLowerCase().indexOf(place.toLowerCase()) > -1)
				return "!" + zones[i];
		}
		return null;
	}
	
	public boolean execute(ServerMessage m, User u, String command)
			throws IOException {
		Pattern p = Pattern.compile("^ *time *(.+)?\\?? *$");
		Matcher mat = p.matcher(command);
		
		if (!mat.find())
			return false;
		
		if (!checkPriv(m, u, 'q'))
			return true;

		String[] places;
		
		String place = mat.group(1);
		String guess = "";
		
		if (place != null) {
			place = guessTimeZone(place);
			if (place == null) {
				m.replyChannel(m.prefix.getClient() + ", I don't know of a place called "
						+ mat.group(1));
				return true;
			}
			if (place.charAt(0) == '!') {
				guess = "I guess you mean ";
				place = place.substring(1);
			}
			places = new String[] { guessTimeZone(place) };
		} else {
			places = new String[] {
				"America/Los_Angeles",
				"America/New_York",
				"Europe/London",
				"Europe/Berlin",
				"Asia/Tokyo",
				"Australia/Sydney"
			};
		}

		String result = "";
		
		Date now = new Date();
		SimpleDateFormat d = new SimpleDateFormat("HH:mm ('UTC' Z)");
		
		for (int i = 0; i < places.length; ++i) {
			d.setTimeZone(TimeZone.getTimeZone(places[i]));
			result += places[i].substring(places[i].indexOf('/') + 1).replaceAll("_", " ") 
				+ ": " + d.format(now);
			if (i < places.length - 1)
				result += " | ";
		}

		m.replyChannel(m.prefix.getClient() + ", " + guess + result);
		return true;
	}

}
