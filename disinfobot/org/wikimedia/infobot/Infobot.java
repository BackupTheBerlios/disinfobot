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
 * $Id: Infobot.java,v 1.1 2004/11/22 18:27:57 kate Exp $
 */

package org.wikimedia.infobot;

import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.wikimedia.infobot.handlers.Handler;
import org.wikimedia.infobot.irc.IRCConnection;
import org.wikimedia.infobot.irc.ServerMessage;
import org.wikimedia.infobot.irchandlers.IRCHandler;

import com.sleepycat.util.RuntimeExceptionWrapper;

public class Infobot {
	public static final 		String version = "Release 1";
	public static 				Config cfg;
	public static 				IRCConnection currentServer;
	
	public static FactoidDataStore	ds;
	public static SeenDataStore		seends;
	public static int			numQueries, numAdditions;
	public static String		startupTime;
	
	public static List			handlers;
	private static PrintWriter	logfile;
	
	protected static void sortHandlersList() {
		List normals = new ArrayList();
		List everythings = new ArrayList();
		
		for (Iterator i = handlers.iterator(); i.hasNext();) {
			Handler h = (Handler) i.next();
			if (h.getMatchesEverything())
				everythings.add(h);
			else
				normals.add(h);
		}
		
		normals.addAll(everythings);
		handlers = normals;
	}
	
	public static void loadHandler(String handler) 
	throws ClassNotFoundException, NoSuchMethodException,
	InstantiationException, InvocationTargetException,
	IllegalAccessException {
		unloadHandler(handler);
		String classname = handler;
		//ClassLoader l = ClassLoader.getSystemClassLoader();
		FileClassLoader l = new FileClassLoader();
		Class c = l.findClass(classname);
		Constructor con = c.getConstructor(new Class[] {});
		Object ob = con.newInstance(new Object[] {});
		if (ob == null) {
			logMsg("newInstance() returned null.");
			return;
		}
		handlers.add((Handler)ob);
		
		sortHandlersList();
	}
	
	public static boolean unloadHandler(String handler) {
		int i = 0;
		for (Iterator it = handlers.iterator(); it.hasNext();) {
			Handler h = (Handler) it.next();
			if (h.getClass().getName().equals(handler)) {
				handlers.remove(i);
				return true;
			}
			++i;
		}
		return false;
	}
	
	public static void logMsg(String msg) {
		SimpleDateFormat d = new SimpleDateFormat("d-MMM-yyyy HH:mm:ss");
		String m = d.format(new Date()) + ": " + msg;
		System.out.println(m);
		logfile.println(m);
	}

	public static void initialiseConfiguration() throws FileNotFoundException,
	IOException, ClassNotFoundException, Exception {
		cfg = null;
		ds = null;
		
		cfg = new Config("infobot.xml");
		ds = new FactoidDataStore();
		seends = new SeenDataStore();
	}
	
	public static void main(String[] args) 
	throws ClassNotFoundException, IOException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
		logfile = new PrintWriter(new FileWriter("infobot.log", true), true);
		logMsg("Infobot " + version);
		logMsg("Initialising configuration...");
		handlers = new ArrayList();
		try {
			initialiseConfiguration();
		} catch (Exception e) {
			logMsg("An unexpected error occured loading the configuration: " + e);
			logStackTrace(e);
			logMsg("Cannot continue after this error, aborting...");
			System.exit(1);
		}
		
		Date now = new Date();
		SimpleDateFormat d = new SimpleDateFormat("EEE, d-MMM-yyyy HH:mm:ss Z");
		startupTime = d.format(now);
		numQueries = numAdditions = 0;
		
		if (args.length > 0 && args[0].equals("-importdb")) {
			if (args.length < 2) {
				System.out.println("Missing argument.");
				return;
			}
			String fromfile = args[1];
			ds.importAncientDatabase(fromfile);
			return;
		}
		
		logMsg("Starting...");
		
		while (true) {
			try {
				currentServer = new IRCConnection();
				currentServer.setChannels(cfg.getChannels());
				currentServer.setNickname(cfg.getNickname());
				currentServer.setRealname("Java Infobot");
				currentServer.setServers(cfg.getServers());
				currentServer.connect();
				while (true) {
					parseOneMessage();
				}
			} catch (IOException e) {
				logMsg("Server connection error: " + e);
				try {Thread.sleep(5000);}
				catch (Exception f) {}
				continue;
			}
		}
	}

	protected static void handleCTCP(ServerMessage m) throws IOException {
		String ctcp = ((String)m.arguments.get(1)).substring(1);
		ctcp = ctcp.substring(0, ctcp.length() - 1);
		StringTokenizer st = new StringTokenizer(ctcp);
		String ctcptype = st.nextToken();
		String rest = "";

		if (ctcptype.length() < ctcp.length())
			rest = ctcp.substring(ctcptype.length() + 1);

		if (ctcptype.equals("ACTION"))
			return;
		
		logMsg("CTCP " + ctcptype + " from " + m.prefix.getPrefix());
		if (ctcptype.equals("PING")) {
			m.ctcpReply("PING", rest);
			return;
		} else if (ctcptype.equals("VERSION")) {
			m.ctcpReply("VERSION", "Disinfobot " 
					+ version + " <kate.turner@gmail.com>");
			return;
		}
		return;
	}

	protected static void parseOneMessage() 
	throws IOException, SecurityException, NoSuchMethodException, 
	IllegalArgumentException, InstantiationException, IllegalAccessException, 
	InvocationTargetException {
		ServerMessage m = currentServer.readMessage();
		if (m.type == ServerMessage.T_CTCP) {
			handleCTCP(m);
			return;
		}
		Constructor con = IRCHandler.findHandlerForCommand(m.command);
		if (con == null)
			return;
		
		IRCHandler handler = (IRCHandler) con.newInstance(
				new Object[] {
						m, currentServer
				}
			);
		if (handler == null)
			return;
		handler.start();
	}

	public static void logStackTrace(Throwable e) {
		logMsg("INTERNAL ERROR: Unexpected exception caught (will try to continue anyway).");
		logMsg("Logged exception of type " + e.getClass().getName() + ": "
				+ e.getMessage());
		logMsg("--- Exception stack trace follows ---");
		StackTraceElement[] elems = e.getStackTrace();
		for (int i = 0; i < elems.length; ++i)
			logMsg("   " + elems[i].toString());
		logMsg("--- End exception stack trace ---");
		
		Throwable t = e.getCause();
		if (t == null) {
			try {
				RuntimeExceptionWrapper re = (RuntimeExceptionWrapper) e;
				if (re == null)
					return;
				t = re.getCause();
			} catch (ClassCastException cce) {
				return;
			}
		}
		if (t == null)
			return;
		logMsg("--- Root cause: " + t.getClass().getName() + ": " + t.getMessage() +
				" (stack trace follows)");
		logStackTrace(t);
	}
}
