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
 * $Id: WeatherHandler.java,v 1.1 2004/12/20 07:28:26 kate Exp $
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

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Node;
import org.wikimedia.infobot.Infobot;
import org.wikimedia.infobot.User;
import org.wikimedia.infobot.irc.ServerMessage;
import org.xml.sax.InputSource;

import com.icl.saxon.Context;
import com.icl.saxon.expr.Expression;
import com.icl.saxon.expr.NodeSetValue;
import com.icl.saxon.expr.StandaloneContext;
import com.icl.saxon.om.Axis;
import com.icl.saxon.om.AxisEnumeration;
import com.icl.saxon.om.DocumentInfo;
import com.icl.saxon.om.NamePool;
import com.icl.saxon.om.NodeEnumeration;
import com.icl.saxon.om.NodeInfo;
import com.icl.saxon.pattern.AnyNodeTest;

/**
 * @author Kate Turner
 *
 */
public class WeatherHandler extends Handler {
	public static String f2c (String f) {
		String r = new Double((Double.valueOf(f).doubleValue() - 32) / 1.8).toString();;
		int i = r.indexOf('.');
		if (i != -1) {
			r = r.substring(0, i + 2);
		}
		return r;
	}
	
	public boolean execute(ServerMessage m, User u, String command) 
	throws IOException {
		Pattern p = Pattern.compile("^(weather *) +(for +)?+(.*?) *\\??$");
		Matcher mat = p.matcher(command);
		
		if (!mat.find())
			return false;

		if (!checkPriv(m, u, 'q'))
			return true;
		
		String nick = m.prefix.getClient();
		String target = nick;
		String locname = mat.group(3);
		// get the location code first
		
		// http://xoap.weather.com/search/search?where=atlanta
		String urlstr = "http://xoap.weather.com/search/search?where="
			+ URLEncoder.encode(locname, "UTF-8"); 
		URL url = new URL(urlstr);
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		conn.setUseCaches(false);
		conn.setInstanceFollowRedirects(false);
		conn.setRequestProperty("User-Agent", 
				"Wikipedia-Java-Infobot/" + Infobot.version + " (contact: kate.turner@gmail.com)");
		conn.connect();
		int i = conn.getResponseCode();
		if (i != 200) {
			m.replyChannel("sorry " + nick + ", there seems to have been an error: "
					+ i + " " + conn.getResponseMessage());
			return true;
		}
		DataInputStream dis = new DataInputStream(new BufferedInputStream(conn.getInputStream()));
		String articletext;
		StandaloneContext standaloneContext;
		Context context;
		String loccode = "";
		
		try {
	        DocumentBuilderFactory factory = new com.icl.saxon.om.DocumentBuilderFactoryImpl();
			DocumentBuilder builder = factory.newDocumentBuilder();
	        InputSource inputSrc = new InputSource(dis);
	        Node xmlDoc = builder.parse(inputSrc);
	        NodeInfo nodeInfo = (NodeInfo) xmlDoc;
	        context = new Context();
	        context.setContextNode(nodeInfo);
	        DocumentInfo docInfo = nodeInfo.getDocumentRoot();
	        NamePool namePool = docInfo.getNamePool();
	        standaloneContext = new StandaloneContext(namePool);
			Expression expr = Expression.make("/search/loc", standaloneContext);
			NodeSetValue resultSet = expr.evaluateAsNodeSet(context);
			NodeEnumeration result = resultSet.enumerate();
			if (result.hasMoreElements()) {
				NodeInfo curnode = result.nextElement();
				loccode = curnode.getAttributeValue("", "id");
				AxisEnumeration axis = curnode.getEnumeration(Axis.CHILD, AnyNodeTest.getInstance());
				if (axis.hasMoreElements()) {
					NodeInfo n = axis.nextElement();
					locname = n.getStringValue();
				}
			}
		} catch (Exception e) {
			m.replyChannel("sorry " + nick + ", there seems to have been an error:"
					+ e.getMessage());
			return true;
		}
		if (loccode.length() == 0) {
			m.replyChannel("sorry " + nick + ", I couldn't find a place called "
					+ locname);
			return true;
		}
		Infobot.logMsg("Location code: [" + loccode + "], name=[" + locname + "]");
		// /weather/local/USGA0028?cc=*&dayf=1&prod=xoap&par=foo&key=foo
		urlstr = "http://xoap.weather.com/weather/local/"
			+ URLEncoder.encode(loccode, "UTF-8") + 
			"?cc=*&dayf=1&prod=xoap&par=foo&key=foo";
		url = new URL(urlstr);
		conn = (HttpURLConnection) url.openConnection();
		conn.setUseCaches(false);
		conn.setInstanceFollowRedirects(false);
		conn.setRequestProperty("User-Agent", 
				"Wikipedia-Java-Infobot/" + Infobot.version + " (contact: kate.turner@gmail.com)");
		conn.connect();
		i = conn.getResponseCode();
		if (i != 200) {
			m.replyChannel("sorry " + nick + ", there seems to have been an error: "
					+ i + " " + conn.getResponseMessage());
			return true;
		}
		dis = new DataInputStream(new BufferedInputStream(conn.getInputStream()));
		String cond, fcast, when, tempunits, temp, flik, hmid, wind, gusts, 
			windunits, visib, visunits;
		
		try {
	        DocumentBuilderFactory factory = new com.icl.saxon.om.DocumentBuilderFactoryImpl();
			DocumentBuilder builder = factory.newDocumentBuilder();
	        InputSource inputSrc = new InputSource(dis);
	        Node xmlDoc = builder.parse(inputSrc);
	        NodeInfo nodeInfo = (NodeInfo) xmlDoc;
	        context = new Context();
	        context.setContextNode(nodeInfo);
	        DocumentInfo docInfo = nodeInfo.getDocumentRoot();
	        NamePool namePool = docInfo.getNamePool();
	        standaloneContext = new StandaloneContext(namePool);
			Expression expr = Expression.make("/weather/cc/t", standaloneContext);
			cond = expr.evaluateAsString(context);
			tempunits = Expression.make("/weather/head/ut", standaloneContext).evaluateAsString(context);
			temp = Expression.make("/weather/cc/tmp", standaloneContext).evaluateAsString(context);
			flik = Expression.make("/weather/cc/flik", standaloneContext).evaluateAsString(context);
			hmid = Expression.make("/weather/cc/hmid", standaloneContext).evaluateAsString(context);
			visib = Expression.make("/weather/cc/vis", standaloneContext).evaluateAsString(context);
			visunits = Expression.make("/weather/head/ud", standaloneContext).evaluateAsString(context);
			wind = Expression.make("/weather/cc/wind/s", standaloneContext).evaluateAsString(context);
			gusts = Expression.make("/weather/cc/wind/gust", standaloneContext).evaluateAsString(context);
			windunits = Expression.make("/weather/head/us", standaloneContext).evaluateAsString(context);
			when = Expression.make("/weather/cc/lsup", standaloneContext).evaluateAsString(context);			
		} catch (Exception e) {
			m.replyChannel("sorry " + nick + ", there seems to have been an error:"
					+ e.getMessage());
			return true;
		}
		dis.close();
		String full;
		if (tempunits.equals("F")) {
			tempunits = "C";
			temp = f2c(temp);
			flik = f2c(flik);
		}
		full = cond + " (" + temp + tempunits;
		if (!temp.equals(flik))
			full += "; feels like " + flik + tempunits;
		full += "); ";
		full += "Humidity: " + hmid + "%, visibility " + visib.toLowerCase();
		if (visib.matches("^[0-9.]+$"))
			full += visunits;
		full += ", " + wind;
		if (wind.matches("^[0-9.]+$"))
			full += " " + windunits;
		full += " winds";
		if (!gusts.equals("N/A"))
			full += " (" + gusts + windunits + " gusts)";
		m.replyChannel(nick + ", weather for " + locname + " ("+loccode+") at "
				+ when + ": " + full);
		return true;
	}
}
