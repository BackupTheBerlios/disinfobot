package JLinAlg;

import JLinAlg.FieldElement;


// logical AND of two FieldElements
class AndOperator implements DyadicOperator {

	public FieldElement apply(FieldElement x, FieldElement y) {
		return (x.isZero() || y.isZero()) ? x.zero() : x.one();
	}
}
