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
 * $Id: BigDecimalWrapper.java,v 1.1 2004/11/22 18:27:57 kate Exp $
 */
package org.wikimedia.infobot;

import java.math.BigDecimal;

import JLinAlg.FieldElement;

/**
 * @author Kate Turner
 *
 */
public class BigDecimalWrapper extends FieldElement {
	BigDecimal value;

	public BigDecimal getValue() {
		return value;
	}
	public BigDecimalWrapper(BigDecimal val) {
		this.value = new BigDecimal(val.unscaledValue(), val.scale());
	}
	
	public FieldElement add(FieldElement val) {
		return new BigDecimalWrapper(value.add(((BigDecimalWrapper)val).value));
	}

	public FieldElement multiply(FieldElement val) {
		return new BigDecimalWrapper(value.multiply(((BigDecimalWrapper)val).value));
	}

	public FieldElement zero() {
		return new BigDecimalWrapper(BigDecimal.valueOf(0));
	}

	public FieldElement one() {
		return new BigDecimalWrapper(BigDecimal.valueOf(1));
	}

	public FieldElement negate() {
		return new BigDecimalWrapper(value.negate());
	}

	public FieldElement invert() {
		return new BigDecimalWrapper(this.value.divide(
				BigDecimal.valueOf(1), BigDecimal.ROUND_HALF_UP));
	}

	public int compareTo(Object o) {
		return value.compareTo((BigDecimal)o);
	}

	public FieldElement instance(double dval) {
		return new BigDecimalWrapper(new BigDecimal(dval));
	}
}
