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
 * $Id: WikipediaLookup.java,v 1.3 2004/12/21 11:12:02 kate Exp $
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

import org.nfunk.jep.EvaluatorVisitor;
import org.w3c.dom.Node;
import org.wikimedia.infobot.Infobot;
import org.wikimedia.infobot.User;
import org.wikimedia.infobot.irc.ServerMessage;
import org.xml.sax.InputSource;

import com.icl.saxon.Context;
import com.icl.saxon.expr.Expression;
import com.icl.saxon.expr.StandaloneContext;
import com.icl.saxon.om.DocumentInfo;
import com.icl.saxon.om.NamePool;
import com.icl.saxon.om.NodeInfo;

/**
 * @author Kate Turner
 *
 */
public class WikipediaLookup extends Handler {
	public static class WpQuery {
		String realtitle = "";

		public String getRealTitle() {
			return realtitle;
		}
		public String fetchWpArticle(ServerMessage m, String nick, 
				String host, String name) throws IOException {
			String urlstr = "http://"+host+"/wiki/Special:Export/"
				+ URLEncoder.encode(name, "ISO-8859-1");
			URL url = new URL(urlstr);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setUseCaches(false);
			conn.setInstanceFollowRedirects(false);
			conn.setRequestProperty("User-Agent", 
					"Wikipedia-Java-Infobot/" + Infobot.version + " (contact: kate.turner@gmail.com)");
			conn.connect();
			int i = conn.getResponseCode();
			if (i == 301 || i == 302) {
				String newurl = conn.getHeaderField("Location");
				url = new URL(newurl);
				conn = (HttpURLConnection) url.openConnection();
				conn.setUseCaches(false);
				conn.setInstanceFollowRedirects(false);
				conn.setRequestProperty("User-Agent", 
						"Wikipedia-Java-Infobot/" + Infobot.version + " (contact: kate.turner@gmail.com)");
				conn.connect();	
				i = conn.getResponseCode();
			}
			if (i != 200) {
				m.replyChannel("sorry " + nick + ", there seems to have been an error: "
						+ i + " " + conn.getResponseMessage());
				return null;
			}
			DataInputStream dis = new DataInputStream(new BufferedInputStream(conn.getInputStream()));
			String articletext;
			StandaloneContext standaloneContext;
			Context context;
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
				articletext = Expression.make("/mediawiki/page/revision/text", 
						standaloneContext).evaluateAsString(context);
				realtitle = Expression.make("/mediawiki/page/title",
						standaloneContext).evaluateAsString(context);
			} catch (Exception e) {
				m.replyChannel("sorry " + nick + ", there seems to have been an error:"
						+ e.getMessage());
				return null;
			}
			dis.close();
			return articletext;
		}
	}
	
	public boolean execute(ServerMessage m, User u, String command) throws IOException {
		Pattern p = Pattern.compile("^(what *is) +(.*?) *\\??$");
		Matcher mat = p.matcher(command);
		
		if (!mat.find())
			return false;

		if (!checkPriv(m, u, 'q'))
			return true;
		
		String trail = ".wikipedia.org";
		String host = "en";

		String nick = m.prefix.getClient();
		String target = nick;
		String articlename = mat.group(2);
		p = Pattern.compile("^([a-z-]+):.*");
		mat = p.matcher(articlename);
		if (mat.find()) {
			host = mat.group(1);
			articlename = articlename.replaceAll("^([a-z-]+):", "");
		}
		host = host + trail;
		WpQuery qy = new WpQuery();
		String articletext = qy.fetchWpArticle(m, nick, host, articlename);
		if (articletext == null)
			return true;
		p = Pattern.compile("^#[rR][eE][dD][iI][rR][eE][cC][tT] *\\[\\[([^]]*)\\]\\].*");
		mat = p.matcher(articletext);
		if (mat.find()) {
			articlename = mat.group(1);
			articletext = qy.fetchWpArticle(m, nick, host, articlename);
			if (articletext == null)
				return true;
		}
		//Infobot.logMsg("text: [" + articletext + "]");
		if (articletext.trim().length() == 0) {
			m.replyChannel("sorry " + nick + ", I don't know what " + articlename
					+ " is.");
			return true;
		}
		articletext = new String(articletext.getBytes("UTF-8"), "ISO-8859-1");
		if (articletext.indexOf("{{disambig}}") != -1) {
			String[] links = articletext.split("\\[\\[");
			String result = "";
			for (int i = 0; i < links.length; ++i) {
				int j = links[i].indexOf("]]");
				if (j == -1)
					continue;
				links[i] = links[i].substring(0, j);
				if (links[i].startsWith("Category"))
					continue;
				if (links[i].indexOf(":") != -1)
					continue;
				int pipe = links[i].indexOf("|");
				String link = links[i];
				if (pipe != -1)
					link = links[i].substring(0, pipe);
				result += ('"'+link+'"'+", ");
			}
			result = result.substring(0, result.length() - 2);
			m.replyChannel(nick + ", " + articlename + " is a disambiguation page. " +
					"It contains the following links: " + result);
			return true;
		}
		
		// remove comments
		//articletext = articletext.replaceAll("<!--(.*?)-->", "");
		// remove links
		int i = 0, j, k;
		i = articletext.indexOf("[[Image:");
		if (i == -1) i = articletext.indexOf("[[image:");
		int l = i;
		while (i > -1) {
			//i = articletext.indexOf("[[", i);
			//if (l == -1) l = i;
			//if (i == -1) break;
			j = articletext.indexOf("[[", i + 2);
			k = articletext.indexOf("]]", i + 2);
			Infobot.logMsg("image find: j="+j+" k="+k);
			if (j != -1 && j < k && k > -1) {
				i = k;
				continue;
			} else {
				Infobot.logMsg("image part: [" + articletext.substring(l, k) + "]");
				if (k == -1)
					articletext = articletext.substring(0, l);
				else
					articletext = articletext.substring(0, l) + 
						articletext.substring(k + 2);
				i = articletext.indexOf("[[Image:");
				if (i == -1) i = articletext.indexOf("[[image:");		
				l = i;
			}
		}
		//articletext = articletext.replaceAll(
		//		"\\[\\[([iI]mage|[mM]edia):.*?(\\[\\[.*?(\\|.*?)\\]\\]){0,}\\]\\]", "");
		// trim
		articletext = articletext.trim();
		// take the first line
		String reply = articletext;
		//Infobot.logMsg("reply: [" + reply.substring(0,500) + "]");
		// formatting
		while ((i = reply.indexOf("<!--")) != -1) {
			if ((j = reply.indexOf("-->")) == -1)
				break;
			reply = reply.substring(0, i) + reply.substring(j + 4);
		}
		while ((i = reply.indexOf("{{Taxobox_begin")) != -1) {
			if ((j = reply.indexOf("Taxobox_end}}, i")) == -1)
				break;
			reply = reply.substring(0, i) + reply.substring(j + 13);
		}
		reply = reply.replaceAll(" *\\{\\{+([^}]*?)\\}\\}+ *", "");
		while ((i = reply.indexOf("{{")) != -1) {
			if ((j = reply.indexOf("}}", i)) == -1)
				break;
			reply = reply.substring(0, i) + reply.substring(j + 3);
		}
		while ((i = reply.indexOf("{|")) != -1) {
			if ((j = reply.indexOf("|}", i)) == -1)
				break;
			reply = reply.substring(0, i) + reply.substring(j + 3);
		}
		while ((i = reply.indexOf("<table")) != -1) {
			if ((j = reply.indexOf("</table>", i)) == -1)
				break;
			reply = reply.substring(0, i) + reply.substring(j + 8);
		}
		//Infobot.logMsg("after taxobox: [" + reply.substring(0,500) + "]");
		reply = reply.replaceAll("\\{\\|(.*?)\\|\\}", "");
		//reply = reply.replaceAll("\\{\\{Taxobox[_ ]begin(.*?)Taxobox[ _]end\\}\\}", "");
		reply = reply.replaceAll("\\[\\[[A-Za-z_-]+:([^|]+?)\\]\\]", "");
		reply = reply.replaceAll("\\[\\[([^|]+?)\\]\\]", "$1");
		reply = reply.replaceAll("\\[\\[([^|]+\\|)(.*?)\\]\\]", "$2");
		//Infobot.logMsg("after tables/templates: [" + reply.substring(0,500) + "]");
		// any lines starting with '' are probably disambig
		reply = reply.replaceAll("(^|\n):*''[^'].*\n", "");
		reply = reply.replaceAll("^----.*", "");
		reply = reply.trim();
		//Infobot.logMsg("after trim: [" + reply.substring(0,500) + "]");
		reply = reply.replaceAll("'''''", ""/*"\002"*/);
		reply = reply.replaceAll("('''|</?[bB]>)", ""/*"\002"*/);
		reply = reply.replaceAll("''", "");
		reply = reply.replaceAll("</?[uU]>", ""/*"\007"*/);
		reply = reply.replaceAll(" *==+ *([^=]+?) *==+ *", "\002$1:\002");
		//Infobot.logMsg("after text weight: [" + reply.substring(0,500) + "]");
		reply = replaceEntities(reply);
		//Infobot.logMsg("after entities: [" + reply.substring(0,500) + "]");
		//int end = reply.indexOf('\n');
		//if (end > -1)
		//	reply = reply.substring(0, end);
		// join the rest together
		reply = reply.replaceAll("\n", " ");
		reply = reply.replaceAll(" +", " ");
		String url = "http://"+host+"/wiki/" + URLEncoder.encode(qy.getRealTitle());
		int maxlen = 400 - url.length();
		if (reply.length() > maxlen)
			reply = reply.substring(0, maxlen) + "...";
		reply += " [" + url + "]";
		Infobot.logMsg("Sending reply: [" + reply + "]");
		m.replyChannel(nick + ", " + reply);
		return true;
	}

	static String htmlentities[][] = {
			{"nbsp", "160"},
			{"iexcl", "161"},
			{"cent", "162"},
			{"pound", "163"},
			{"curren", "164"},
			{"yen", "165"},
			{"brvbar", "166"},
			{"sect", "167"},
			{"uml", "168"},
			{"copy", "169"},
			{"ordf", "170"},
			{"laquo", "171"},
			{"not", "172"},
			{"shy", "173"},
			{"reg", "174"},
			{"macr", "175"},
			{"deg", "176"},
			{"plusmn", "177"},
			{"sup2", "178"},
			{"sup3", "179"},
			{"acute", "180"},
			{"micro", "181"},
			{"para", "182"},
			{"middot", "183"},
			{"cedil", "184"},
			{"sup1", "185"},
			{"ordm", "186"},
			{"raquo", "187"},
			{"frac14", "188"},
			{"frac12", "189"},
			{"frac34", "190"},
			{"iquest", "191"},
			{"Agrave", "192"},
			{"Aacute", "193"},
			{"Acirc", "194"},
			{"Atilde", "195"},
			{"Auml", "196"},
			{"Aring", "197"},
			{"AElig", "198"},
			{"Ccedil", "199"},
			{"Egrave", "200"},
			{"Eacute", "201"},
			{"Ecirc", "202"},
			{"Euml", "203"},
			{"Igrave", "204"},
			{"Iacute", "205"},
			{"Icirc", "206"},
			{"Iuml", "207"},
			{"ETH", "208"},
			{"Ntilde", "209"},
			{"Ograve", "210"},
			{"Oacute", "211"},
			{"Ocirc", "212"},
			{"Otilde", "213"},
			{"Ouml", "214"},
			{"times", "215"},
			{"Oslash", "216"},
			{"Ugrave", "217"},
			{"Uacute", "218"},
			{"Ucirc", "219"},
			{"Uuml", "220"},
			{"Yacute", "221"},
			{"THORN", "222"},
			{"szlig", "223"},
			{"agrave", "224"},
			{"aacute", "225"},
			{"acirc", "226"},
			{"atilde", "227"},
			{"auml", "228"},
			{"aring", "229"},
			{"aelig", "230"},
			{"ccedil", "231"},
			{"egrave", "232"},
			{"eacute", "233"},
			{"ecirc", "234"},
			{"euml", "235"},
			{"igrave", "236"},
			{"iacute", "237"},
			{"icirc", "238"},
			{"iuml", "239"},
			{"eth", "240"},
			{"ntilde", "241"},
			{"ograve", "242"},
			{"oacute", "243"},
			{"ocirc", "244"},
			{"otilde", "245"},
			{"ouml", "246"},
			{"divide", "247"},
			{"oslash", "248"},
			{"ugrave", "249"},
			{"uacute", "250"},
			{"ucirc", "251"},
			{"uuml", "252"},
			{"yacute", "253"},
			{"thorn", "254"},
			{"yuml", "255"},
			{"ouml", "246"},
			{"divide", "247"},
			{"oslash", "248"},
			{"ugrave", "249"},
			{"uacute", "250"},
			{"ucirc", "251"},
			{"uuml", "252"},
			{"yacute", "253"},
			{"thorn", "254"},
			{"yuml", "255"},
			{"quot", "34"},
			{"amp", "38"},
			{"lt", "60"},
			{"gt", "62"},
			{"OElig", "338"},
			{"oelig", "339"},
			{"Scaron", "352"},
			{"scaron", "353"},
			{"Yuml", "376"},
			{"circ", "710"},
			{"tilde", "732"},
			{"ensp", "8194"},
			{"emsp", "8195"},
			{"thinsp", "8201"},
			{"zwnj", "8204"},
			{"zwj", "8205"},
			{"lrm", "8206"},
			{"rlm", "8207"},
			{"ndash", "8211"},
			{"mdash", "8212"},
			{"lsquo", "8216"},
			{"rsquo", "8217"},
			{"sbquo", "8218"},
			{"ldquo", "8220"},
			{"rdquo", "8221"},
			{"bdquo", "8222"},
			{"dagger", "8224"},
			{"Dagger", "8225"},
			{"permil", "8240"},
			{"lsaquo", "8249"},
			{"rsaquo", "8250"},
			{"euro", "8364"},
			{"fnof", "402"},
			{"Alpha", "913"},
			{"Beta", "914"},
			{"Gamma", "915"},
			{"Delta", "916"},
			{"Epsilon", "917"},
			{"Zeta", "918"},
			{"Eta", "919"},
			{"Theta", "920"},
			{"Iota", "921"},
			{"Kappa", "922"},
			{"Lambda", "923"},
			{"Mu", "924"},
			{"Nu", "925"},
			{"Xi", "926"},
			{"Omicron", "927"},
			{"Pi", "928"},
			{"Rho", "929"},
			{"Sigma", "931"},
			{"Tau", "932"},
			{"Upsilon", "933"},
			{"Phi", "934"},
			{"Chi", "935"},
			{"Psi", "936"},
			{"Omega", "937"},
			{"alpha", "945"},
			{"beta", "946"},
			{"gamma", "947"},
			{"delta", "948"},
			{"epsilon", "949"},
			{"zeta", "950"},
			{"eta", "951"},
			{"theta", "952"},
			{"iota", "953"},
			{"kappa", "954"},
			{"lambda", "955"},
			{"mu", "956"},
			{"nu", "957"},
			{"xi", "958"},
			{"omicron", "959"},
			{"pi", "960"},
			{"rho", "961"},
			{"sigmaf", "962"},
			{"sigma", "963"},
			{"tau", "964"},
			{"upsilon", "965"},
			{"phi", "966"},
			{"chi", "967"},
			{"psi", "968"},
			{"omega", "969"},
			{"thetasym", "977"},
			{"upsih", "978"},
			{"piv", "982"},
			{"bull", "8226"},
			{"hellip", "8230"},
			{"prime", "8242"},
			{"Prime", "8243"},
			{"oline", "8254"},
			{"frasl", "8260"},
			{"weierp", "8472"},
			{"image", "8465"},
			{"real", "8476"},
			{"trade", "8482"},
			{"alefsym", "8501"},
			{"larr", "8592"},
			{"uarr", "8593"},
			{"rarr", "8594"},
			{"darr", "8595"},
			{"harr", "8596"},
			{"crarr", "8629"},
			{"lArr", "8656"},
			{"uArr", "8657"},
			{"rArr", "8658"},
			{"dArr", "8659"},
			{"hArr", "8660"},
			{"forall", "8704"},
			{"part", "8706"},
			{"exist", "8707"},
			{"empty", "8709"},
			{"nabla", "8711"},
			{"isin", "8712"},
			{"notin", "8713"},
			{"ni", "8715"},
			{"prod", "8719"},
			{"sum", "8721"},
			{"minus", "8722"},
			{"lowast", "8727"},
			{"radic", "8730"},
			{"prop", "8733"},
			{"infin", "8734"},
			{"ang", "8736"},
			{"and", "8743"},
			{"or", "8744"},
			{"cap", "8745"},
			{"cup", "8746"},
			{"int", "8747"},
			{"there4", "8756"},
			{"sim", "8764"},
			{"cong", "8773"},
			{"asymp", "8776"},
			{"ne", "8800"},
			{"equiv", "8801"},
			{"le", "8804"},
			{"ge", "8805"},
			{"sub", "8834"},
			{"sup", "8835"},
			{"nsub", "8836"},
			{"sube", "8838"},
			{"supe", "8839"},
			{"oplus", "8853"},
			{"otimes", "8855"},
			{"perp", "8869"},
			{"sdot", "8901"},
			{"lceil", "8968"},
			{"rceil", "8969"},
			{"lfloor", "8970"},
			{"rfloor", "8971"},
			{"lang", "9001"},
			{"rang", "9002"},
			{"loz", "9674"},
			{"spades", "9824"},
			{"clubs", "9827"},
			{"hearts", "9829"},
			{"diams", "9830"},
	};
	

	public static String replaceEntities(String s) {
		int i = 0, j;
		//System.out.println("Doing entities for: [" + s + "]");
		while ((i = s.indexOf("&", i)) != -1) {
			++i;
			j = s.indexOf(";", i);
			if (j == -1)
				break;
			//System.out.println("Found entity at "+i+".."+j+"=[" + s.substring(i,j) + "]");
			if ((j - i) > 10)
				continue;
			String entity = s.substring(i, j);
			if (entity.matches("^#x?[0-9]+$")) {
				int entnum;
				if (entity.charAt(1) == 'x')
					entnum = Integer.valueOf(entity.substring(2), 16).intValue();
				else
					entnum = Integer.valueOf(entity.substring(1), 10).intValue();
				s = s.substring(0, i - 1) + (char)entnum + s.substring(j + 1);
			} else {
				boolean found = false;
				for (int q = 0; q < htmlentities.length; ++q)
					if (entity.equals(htmlentities[q][0])) {
						System.out.println("match: " + htmlentities[q][0]);
						s = s.substring(0, i - 1) +
						(char)(Integer.valueOf(htmlentities[q][1]).intValue())
						+ s.substring(j + 1);
						found = true;
						break;
					}
				if (!found)
					s = s.substring(0, i - 1) + s.substring(j + 1);
			}
			i -= (j - i);
		}
		return s;
	}
}
