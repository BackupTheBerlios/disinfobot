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
 * $Id: Config.java,v 1.1 2004/11/22 18:27:57 kate Exp $
 */

package org.wikimedia.infobot;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Node;
import org.wikimedia.infobot.irchandlers.IRCHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.icl.saxon.Context;
import com.icl.saxon.expr.Expression;
import com.icl.saxon.expr.NodeSetValue;
import com.icl.saxon.expr.StandaloneContext;
import com.icl.saxon.expr.XPathException;
import com.icl.saxon.om.Axis;
import com.icl.saxon.om.AxisEnumeration;
import com.icl.saxon.om.DocumentInfo;
import com.icl.saxon.om.NamePool;
import com.icl.saxon.om.NodeEnumeration;
import com.icl.saxon.om.NodeInfo;
import com.icl.saxon.pattern.AnyNodeTest;

/**
 * Represents a disinfobot XML configuration file.
 * @author Kate Turner
 *
 */
public class Config {
	/**
	 * Represents a single host entry in the configuration.
	 * @author Kate Turner
	 *
	 */
	public static class Server {
		public final String name;
		public final int port;
		
		/**
		 * Construct a new server with the specified name and port.
		 * @param name Hostname of the server
		 * @param port Port number
		 */
		public Server (String name, int port) {
			this.name = name;
			this.port = port;
		}
	}

	/** Our nickname. **/
	private String	nickname = "wikiwiki";
	/** The channel to join once connected. */
	private List		channels;
	/** A list of servers to connect to. */
	private List		servers;
	/** Our real name */
	private String		realname;
	/** The Google API key */
	private String		googleKey;
	/** Privs anonymous users should have */
	private String		defaultprivs;
	
	/**
	 * @return Returns the default privs.
	 */
	public String getDefaultprivs() {
		return defaultprivs;
	}
	/**
	 * @return Returns the googleKey.
	 */
	public String getGoogleKey() {
		return googleKey;
	}
	/**
	 * Parse an infobot.xml file and set up the configuration.
	 * @author Kate Turner
	 *
	 */
	protected class XMLConfigParser {
		StandaloneContext	StandaloneContext;
		Context				context;
		
		/**
		 * Construct a new config parser
		 * @param file Configuration file name
		 * @throws ParserConfigurationException
		 * @throws SAXException
		 * @throws IOException
		 */
		public XMLConfigParser (String file) throws ParserConfigurationException, SAXException, IOException {
	        DocumentBuilderFactory factory = new com.icl.saxon.om.DocumentBuilderFactoryImpl();
			DocumentBuilder builder = factory.newDocumentBuilder();
	        InputSource inputSrc = new InputSource(new FileInputStream(
	        		new File("infobot.xml")));
	        Node xmlDoc = builder.parse(inputSrc);
	        NodeInfo nodeInfo = (NodeInfo) xmlDoc;
	        context = new Context();
	        context.setContextNode(nodeInfo);
	        DocumentInfo docInfo = nodeInfo.getDocumentRoot();
	        NamePool namePool = docInfo.getNamePool();
	        StandaloneContext = new StandaloneContext(namePool);
		}
		
		/**
		 * Return the value of a configuration item
		 * @param value XPath string describing the item.
		 * @return The configuration item as a string
		 * @throws XPathException
		 */
		public String getValue(String value) throws XPathException {
			Expression expr = Expression.make(value, StandaloneContext);
			return expr.evaluateAsString(context);
		}
		
		/**
		 * Return a list of values associated with a certain option
		 * @param value XPath query describing the item.
		 * @return A list of configuration values
		 * @throws XPathException
		 */
		public NodeEnumeration getValues(String value) throws XPathException {
			Expression expr = Expression.make(value, StandaloneContext);
			NodeSetValue resultSet = expr.evaluateAsNodeSet(context);
			NodeEnumeration result = resultSet.enumerate();
			return result;
		}
	}
	
	/**
	 * Construct a new configuration parser from the given file
	 * @param file Configuration file name
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws Exception
	 */
	public Config(String file) throws FileNotFoundException, IOException, Exception {
		channels = new ArrayList();
		servers = new ArrayList();
		User.clearUsers();
		
		XMLConfigParser conf = new XMLConfigParser(file);
		nickname = conf.getValue("/infobot/global/nickname");
		googleKey = conf.getValue("/infobot/global/googlekey");
		realname = conf.getValue("/infobot/global/realname");
		defaultprivs = conf.getValue("/infobot/global/defaultflags");
		
		NodeEnumeration confservers = conf.getValues("/infobot/servers/server");
		while (confservers.hasMoreElements()) {
			NodeInfo curnode = confservers.nextElement();
			AxisEnumeration axis = curnode.getEnumeration(Axis.CHILD, AnyNodeTest.getInstance());
			String servername = null;
			int serverport = 6667;
			while (axis.hasMoreElements()) {
				NodeInfo n = axis.nextElement();
				if (n.getDisplayName().equals("name"))
					servername = n.getStringValue();
				else if (n.getDisplayName().equals("port"))
					serverport = Integer.parseInt(n.getStringValue());
			}
			if (servername == null)
				throw new Exception("Server name not specified");
			Infobot.logMsg("Server: " + servername + ":" + serverport);
			servers.add(servername);
		}

		NodeEnumeration confusers = conf.getValues("/infobot/users/user");
		while (confusers.hasMoreElements()) {
			NodeInfo curnode = confusers.nextElement();
			AxisEnumeration axis = curnode.getEnumeration(Axis.CHILD, AnyNodeTest.getInstance());
			String prefix = null;
			String flags = null;
			String nick = null;
			while (axis.hasMoreElements()) {
				NodeInfo n = axis.nextElement();
				if (n.getDisplayName().equals("hostmask"))
					prefix = n.getStringValue();
				else if (n.getDisplayName().equals("flags"))
					flags = n.getStringValue();
				else if (n.getDisplayName().equals("nickname"))
					nick = n.getStringValue();
			}
			if (prefix == null)
				throw new Exception("User name not specified");
			User.addUser(nick, prefix, flags);
		}

		NodeEnumeration confhandlers = conf.getValues("/infobot/handlers/handler");
		while (confhandlers.hasMoreElements()) {
			NodeInfo curnode = confhandlers.nextElement();
			String handler = curnode.getStringValue();
			Infobot.unloadHandler(handler);
			Infobot.loadHandler(handler);
		}

		NodeEnumeration confirchandlers = conf.getValues("/infobot/irchandlers/irchandler");
		while (confirchandlers.hasMoreElements()) {
			NodeInfo curnode = confirchandlers.nextElement();
			String handler = curnode.getStringValue();
			IRCHandler.loadIRCHandler(handler);
		}

		NodeEnumeration confchans = conf.getValues("/infobot/channels/channel/name");
		while (confchans.hasMoreElements()) {
			NodeInfo curnode = confchans.nextElement();
			String chan = curnode.getStringValue();
			channels.add(chan);
		}
	}
	
	/**
	 * Retrieve the list of channels to join on connect.
	 * @return A list of channels
	 */
	public List getChannels() {
		return channels;
	}
	/**
	 * @return Returns the nickname.
	 */
	public String getNickname() {
		return nickname;
	}
	/**
	 * @return Returns the realname.
	 */
	public String getRealname() {
		return realname;
	}
	/**
	 * @return Returns the servers.
	 */
	public List getServers() {
		return servers;
	}
}
