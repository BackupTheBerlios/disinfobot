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
 * $Id: Factoid.java,v 1.1 2004/11/22 18:27:57 kate Exp $
 */

package org.wikimedia.infobot;

import java.io.Serializable;

/**
 * @author Kate Turner
 *
 */
public class Factoid implements Serializable {
	static final long serialVersionUID = -8481189568090031302L;
	
	protected String	text;
	protected String	author;
	protected int		numQueries;
	
	public Factoid() {
		numQueries = 0;
	}
	
	public void incQueries() {
		++numQueries;
	}
	
	public void setNumQueries(int i) {
		numQueries = i;
	}
	
	public int getNumQueries() {
		return numQueries;
	}
	
	/**
	 * @return Returns the author.
	 */
	public String getAuthor() {
		return author;
	}
	
	/**
	 * @param author The author to set.
	 */
	public void setAuthor(String author) {
		this.author = author;
	}
	/**
	 * @return Returns the text.
	 */
	public String getText() {
		return text;
	}

	/**
	 * @param text The text to set.
	 */
	public void setText(String text) {
		this.text = text;
	}
	
	public String getTextLinks() {
		String s = getText();
		String res = "";
		
		int start = 0, end = 0;
		while ((start = s.indexOf("<link:")) != -1) {
			if ((end = s.indexOf(">", start)) == -1)
				break;
			String fname = s.substring(start + 6, end);
			Factoid f = Infobot.ds.getItem(fname);
			if (f == null) {
				res += s.substring(0, end + 1);
			} else {
				res += s.substring(0, start) + f.getText();
			}
			s = s.substring(end + 1);
		}
		return res + s;
	}
	
	/**
	 * Test whether the given name is sanitory.
	 */
	public static boolean isSanitoryName(String name) {
		for (int i = 0; i < name.length(); ++i)
			if (name.charAt(i) < 32 || name.charAt(i) == 127)
				return false;
		return true;
	}
	
	/**
	 * Return the given string after sanitisation. 
	 * @author Kate Turner
	 *
	 */
	public static String sanitiseName(String name) {
		String res = "";
		for (int i = 0; i < name.length(); ++i) {
			if (name.charAt(i) < 32)
				res += "^" + (char)('@' + name.charAt(i));
			else if (name.charAt(i) == 127)
				res += "^?";
			else
				res += name.charAt(i);
		}
		return res;
	}
}
