package JLinAlg;

import JLinAlg.FieldElement;


// division of two FieldElements
class DivideOperator implements DyadicOperator {

	public FieldElement apply(FieldElement x, FieldElement y) {
		return x.divide(y);
	}

}
