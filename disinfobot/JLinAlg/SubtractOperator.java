package JLinAlg;

import JLinAlg.FieldElement;

// difference of two FieldElements
class SubtractOperator implements DyadicOperator {

	public FieldElement apply(FieldElement x, FieldElement y) {
		return x.subtract(y);
	}

}
