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
 * $Id: ServerConnection.java,v 1.2 2004/11/23 16:13:51 kate Exp $
 */

package org.wikimedia.infobot.irc;

import java.net.InetAddress;
import java.net.Socket;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.io.IOException;

import org.wikimedia.infobot.Config;
import org.wikimedia.infobot.Infobot;

public class ServerConnection {
	Config.Server		serv;
	InetAddress			address;
	Socket				s;
	BufferedReader		in;
	DataOutputStream	out;
	Config				cfg;
	public String		currentNickname;
	
	public ServerConnection(Config cfg, Config.Server attrs) {
		this.serv = attrs;
		this.cfg = cfg;
	}
	
	public void connect() throws IOException {
		try {
			address = InetAddress.getByName(serv.name);
		} catch (Exception e) {
			Infobot.logMsg("Hostname lookup failure: " + e);
			return;
		}
		try {
			s = new Socket(address, serv.port);
		} catch (Exception e) {
			Infobot.logMsg("Connection failure: " + e);
			return;
		}
		
		in = new BufferedReader(new InputStreamReader(s.getInputStream(), "ISO-8859-1"));
		out = new DataOutputStream(s.getOutputStream());
	}
	
	protected String readLine() throws IOException {
		int i;
		char c;
		String s = "";
		while ((i = in.read()) != -1) {
			c = (char) i;
			switch (c) {
			case '\r': case '\n': {
				if (s.length() == 0)
					continue;
				return s;
			}
			default: {
				s += c;
				if (s.length() > 512) {
					throw new IOException("Line from server too long!");
				}
				continue;
			}
			}
		}
		throw new IOException("Server closed connection");
	}
	
	public void writeLine(String s) throws IOException {
		out.writeBytes(s + "\r\n");
		
	}
}

