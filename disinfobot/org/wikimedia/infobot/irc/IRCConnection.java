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
 * $Id: IRCConnection.java,v 1.2 2004/12/21 11:12:02 kate Exp $
 */

package org.wikimedia.infobot.irc;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.wikimedia.infobot.Config;
import org.wikimedia.infobot.Infobot;
import org.wikimedia.infobot.Config.Server;

/**
 * @author Kate Turner
 *
 */
public class IRCConnection {
	private ServerConnection connection;
	private String nickname;
	private List channels;
	private String realname;
	private String currentNickname;
	private String nextNickname;
	private List servers;
	private Iterator thisServer;
	private SendPingThread pthr;
	
	private class SendPingThread extends Thread {
		public boolean doexit = false;
		
		private IRCConnection conn;
		public SendPingThread(IRCConnection c) {
			conn = c;
		}
		
		public void run() {
			while (true) {
				try { Thread.sleep(300000); }
				catch (Exception e) {}
				if (doexit)
					return;
				try {
					conn.writeLine("PING :disinfobot");
				} catch (IOException e) {
					return;
				}
			}
		}
	}
	public IRCConnection() {
	}
	
	public void connect() throws IOException {
		nextNickname = nickname;
		Config.Server serv = getNextServer();
		Infobot.logMsg("Connecting to " + serv.name + ":" + serv.port + "...");
		connection = new ServerConnection(Infobot.cfg, serv);
		try {
			connection.connect();
		} catch (IOException e) {
			Infobot.logMsg("Connection failed: " + e.getMessage());
			throw e;
		}
		
		currentNickname = getNextNickname();
		connection.writeLine("USER infobot me you :" + realname);
		connection.writeLine("NICK " + currentNickname);

		pthr = new SendPingThread(this);
		Infobot.logMsg("Connected.");
	}

	/**
	 * @return Returns the channels.
	 */
	public List getChannels() {
		return channels;
	}
	/**
	 * @param channels The channels to set.
	 */
	public void setChannels(List channels) {
		this.channels = channels;
	}
	/**
	 * @return Returns the nickname.
	 */
	public String getNickname() {
		return nickname;
	}
	/**
	 * @param nickname The nickname to set.
	 */
	public void setNickname(String nickname) {
		this.nickname = nickname;
	}
	/**
	 * @return Returns the connection.
	 */
	public ServerConnection getConnection() {
		return connection;
	}
	
	/**
	 * Increment the current nickname and return a new one.
	 * @return The next nickname to use
	 */
	public String getNextNickname() {
		String r = nextNickname;
		nextNickname += "_";
		return r;
	}
	
	/**
	 * @return Returns the currentNickname.
	 */
	public String getCurrentNickname() {
		return currentNickname;
	}
	/**
	 * Iterate though the server list and return the next server to connect to
	 * @return The next server to connect to.
	 */
	public Server getNextServer() {
		if (thisServer == null || !thisServer.hasNext())
			thisServer = servers.iterator();
		return (Server)thisServer.next();
	}

	/**
	 * @return Returns the realname.
	 */
	public String getRealname() {
		return realname;
	}
	/**
	 * @param realname The realname to set.
	 */
	public void setRealname(String realname) {
		this.realname = realname;
	}
	/**
	 * @return Returns the servers.
	 */
	public List getServers() {
		return servers;
	}
	/**
	 * @param servers The servers to set.
	 */
	public void setServers(List servers) {
		this.servers = servers;
	}
	
	public void joinChannel(String channel) throws IOException {
		writeLine("JOIN " + channel);
	}
	
	public void partChannel(String channel) throws IOException {
		writeLine("PART " + channel);
	}
	
	public void privmsg(String target, String message) throws IOException {
		writeLine("PRIVMSG " + target + " :" + message);
	}

	public void notice(String target, String message) throws IOException {
		writeLine("NOTICE " + target + " :" + message);
	}
	
	public void writeLine(String line) throws IOException {
		try {
			connection.writeLine(line);
		} catch (IOException e) {
			pthr.doexit = true;
			pthr = null;
			throw e;
		}
	}
	
	public ServerMessage readMessage() throws IOException {
		while (true) {
			try {
				ServerMessage m = new ServerMessage(this, connection.readLine());
				if (m.type == ServerMessage.T_PING) {
					connection.writeLine("PONG :" + m.arguments.get(0));
					continue;
				}
				return m;
			} catch (IOException e) {
				pthr.doexit = true;
				pthr = null;
				throw e;
			}
		}
	}
}
