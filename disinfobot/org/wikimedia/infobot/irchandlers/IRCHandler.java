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
 * $Id: IRCHandler.java,v 1.1 2004/11/22 18:27:59 kate Exp $
 */

package org.wikimedia.infobot.irchandlers;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import org.wikimedia.infobot.FileClassLoader;
import org.wikimedia.infobot.Infobot;
import org.wikimedia.infobot.irc.IRCConnection;
import org.wikimedia.infobot.irc.ServerMessage;

/**
 * Abstract class representing a handler for an IRC command.
 * 
 * @author Kate Turner
 *
 */
public abstract class IRCHandler extends Thread {
	private static Map IRCHandlers;
	
	public static void loadIRCHandler(String handler) 
	throws ClassNotFoundException, NoSuchMethodException,
	InstantiationException, InvocationTargetException,
	IllegalAccessException {
		FileClassLoader l = new FileClassLoader();
		Class c = l.findClass(handler);
		Method m = c.getMethod("install", null);
		if (m == null) {
			Infobot.logMsg("Warning: class " + handler + " had no install() method...");
			return;
		}
		m.invoke(null, null);
	}
	
	public static void addIRCHandler(String command, Class c) 
	throws SecurityException, NoSuchMethodException {
		if (IRCHandlers == null)
			IRCHandlers = new HashMap();
		
		if (IRCHandlers.containsKey(command))
			IRCHandlers.remove(command);
		Constructor con = c.getConstructor(new Class[] {
				ServerMessage.class, IRCConnection.class
			}
		);
		if (con == null) {
			Infobot.logMsg("Warning: class " + c.getName() + " has no install() handler...");
			return;
		}
		IRCHandlers.put(command, con);
	}
	
	public static Constructor findHandlerForCommand(String command) {
		return (Constructor) IRCHandlers.get(command);
	}
	
	protected ServerMessage message;
	protected IRCConnection server;
	
	public IRCHandler(ServerMessage message, IRCConnection server) {
		this.message = message;
		this.server = server;
	}
	
	public void run() {
		try {
			execute(server, message);
		} catch (Exception e) {
			Infobot.logMsg("Caught unexpected exception processing IRC command");
			Infobot.logStackTrace(e);
		}
	}

	abstract public void execute(IRCConnection server, ServerMessage msg) throws IOException; 
}
