package JLinAlg;

import JLinAlg.FieldElement;

// difference of two FieldElements
class MultiplyOperator implements DyadicOperator {

	public FieldElement apply(FieldElement x, FieldElement y) {
		return x.multiply(y);
	}

}
