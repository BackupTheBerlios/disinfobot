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
 * $Id: CalcHandler.java,v 1.1 2004/11/22 18:27:58 kate Exp $
 */
package org.wikimedia.infobot.handlers;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.lsmp.djep.matrixJep.MatrixJep;
import org.lsmp.djep.vectorJep.Dimensions;
import org.lsmp.djep.vectorJep.function.UnaryOperatorI;
import org.lsmp.djep.vectorJep.values.MatrixValueI;
import org.lsmp.djep.vectorJep.values.Scaler;
import org.nfunk.jep.Node;
import org.nfunk.jep.ParseException;
import org.nfunk.jep.function.PostfixMathCommand;
import org.wikimedia.infobot.BigDecimalNumberFactory;
import org.wikimedia.infobot.User;
import org.wikimedia.infobot.irc.ServerMessage;

import JLinAlg.DoubleWrapper;
import JLinAlg.FieldElement;
import JLinAlg.Matrix;
import JLinAlg.Vector;

/**
 * @author Kate Turner
 *
 */
public class CalcHandler extends Handler {
	public static class EigFunction extends PostfixMathCommand
	implements UnaryOperatorI {
		private Dimensions dims;
		
		public EigFunction() {
			this.numberOfParameters = 1;
		}
		public Dimensions calcDim(Dimensions dims) {
			return Dimensions.valueOf(dims.getFirstDim());
		}

		public MatrixValueI calcValue(MatrixValueI res, MatrixValueI lhs)
		throws ParseException
		{
			if (!(lhs instanceof org.lsmp.djep.vectorJep.values.Matrix)) {
				throw new ParseException("Can only calculate eigenvalues of matrices");
			}
			Matrix mat2 = jep2jlin((org.lsmp.djep.vectorJep.values.Matrix)lhs);
			Vector result = mat2.eig();
			org.lsmp.djep.vectorJep.values.MVector res2 = jlinvec2jep(result);
			dims = res2.getDim();
			return res2;
		}
	}

	public static class DetFunction extends PostfixMathCommand
	implements UnaryOperatorI {
		public DetFunction() {
			this.numberOfParameters = 1;
		}
		public Dimensions calcDim(Dimensions dims) {
			return Dimensions.ONE;
		}

		public MatrixValueI calcValue(MatrixValueI res, MatrixValueI lhs)
		throws ParseException
		{
			if (!(lhs instanceof org.lsmp.djep.vectorJep.values.Matrix))
				throw new ParseException("I can only find the determinant"
						+ " of matrices");
			Matrix mat2 = jep2jlin((org.lsmp.djep.vectorJep.values.Matrix)lhs);
			FieldElement det = mat2.det();
			if (det == null) 
				throw new ParseException("Matrix without determinant");
			Scaler s = new Scaler();
			s.setEle(0, det);
			return s;
		}
	}

	public static class InvFunction extends PostfixMathCommand
	implements UnaryOperatorI {
		public InvFunction() {
			this.numberOfParameters = 1;
		}
		public Dimensions calcDim(Dimensions dims) {
			return dims;
		}

		public MatrixValueI calcValue(MatrixValueI res, MatrixValueI lhs)
		throws ParseException
		{
			if (!(lhs instanceof org.lsmp.djep.vectorJep.values.Matrix))
				throw new ParseException("I can only find the inverse"
						+ " of matrices");
			Matrix mat2 = jep2jlin((org.lsmp.djep.vectorJep.values.Matrix)lhs);
			Matrix invmat = mat2.inverse();
			if (invmat == null) 
				throw new ParseException("Matrix without inverse");
			return (res = jlin2jep(invmat));
		}
	}

	public static org.lsmp.djep.vectorJep.values.MVector jlinvec2jep(Vector jlinvec) {
		org.lsmp.djep.vectorJep.values.MVector mat2 = new 
		org.lsmp.djep.vectorJep.values.MVector(jlinvec.length());
		for (int i = 1; i <= jlinvec.length(); ++i) {
			mat2.setEle(i - 1, jlinvec.getEntry(i));
		}
		return mat2;
	}
	
	static String formatLinMatrix(Matrix m) {
		String res = "[";
		for (int i = 1; i <= m.getRows(); ++i) {
			res += "[";
			for (int j = 1; j <= m.getCols(); ++j) {
				res += m.get(i, j).toString();
				if (j < m.getCols())
					res += ",";
			}
			res += "]";
			if (i < m.getRows())
				res += ",";
		}
		res += "]";
		return res;
	}

	public static Matrix jep2jlin(org.lsmp.djep.vectorJep.values.Matrix jmat) {
		Matrix mat2 = new Matrix(jmat.getNumRows(), jmat.getNumCols());
		for (int i = 0; i < jmat.getNumRows(); ++i) {
			for (int j = 0; j < jmat.getNumCols(); ++j) {
				mat2.set(i + 1, j + 1, new DoubleWrapper(
						((BigDecimal) jmat.getEle(i, j)).doubleValue()));
			}
		}
		return mat2;
	}

	public static org.lsmp.djep.vectorJep.values.Matrix jlin2jep(Matrix jlmat) {
		org.lsmp.djep.vectorJep.values.Matrix mat2 = new 
			org.lsmp.djep.vectorJep.values.Matrix(jlmat.getRows(), jlmat.getCols());
		for (int i = 1; i <= jlmat.getRows(); ++i) {
			for (int j = 1; j <= jlmat.getCols(); ++j) {
				double o = ((DoubleWrapper)jlmat.get(i, j)).getValue();
				BigDecimal n = new BigDecimal(o);
				mat2.setEle(i - 1, j - 1, n);			
			}
		}
		return mat2;
	}
	
	public boolean execute(ServerMessage m, User u, String command) throws IOException {
		Pattern p = Pattern.compile("^(calc|simplify) +(.*) *$");
		Matcher mat = p.matcher(command);

		if (!mat.find())
			return false;
		
		if (!checkPriv(m, u, 'q'))
			return true;

		String expr = mat.group(2);
		boolean simponly = mat.group(1).equals("simplify");
		String nick = m.prefix.getClient();

		MatrixJep jep = new MatrixJep();
		
		jep.setNumberFactory(new BigDecimalNumberFactory());
		jep.addStandardFunctions();
		jep.addStandardConstants();
		jep.addComplex();
		jep.setImplicitMul(true);
		jep.setAllowAssignment(true);
		jep.setAllowUndeclared(true);
		jep.addFunction("eig", new EigFunction());
		jep.addFunction("inv", new InvFunction());
		jep.addFunction("det", new DetFunction());
		Object value = "<no result>";
		jep.restartParser(expr);

		try {
			Node node;
			while ((node = jep.continueParsing()) != null) {
				if (jep.hasError()) {
					m.replyChannel("sorry " + nick + ", your expression is erroneous: "
							+ jep.getErrorInfo());
					return true;
				}
				Node proc = jep.preprocess(node);
				Node simp = jep.simplify(proc);
				if (simponly)
					value = jep.toString(simp);
				else
					value = jep.evaluate(simp);
			}
		} catch (Exception e) {
			m.replyChannel("sorry " + nick + ", your expression is erroneous: " 
					+ e.getMessage());
			return true;
		}

		String result = value.toString();
		m.replyChannel(nick + ", the answer is " + result);
		return true;
	}
}
