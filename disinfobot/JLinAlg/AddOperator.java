package JLinAlg;

import JLinAlg.FieldElement;


// sum of two FieldElements
class AddOperator implements DyadicOperator {

	public FieldElement apply(FieldElement x, FieldElement y) {
		return x.add(y);
	}

}
