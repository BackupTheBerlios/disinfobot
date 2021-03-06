package JLinAlg;

import JLinAlg.FieldElement;

// class to return FieldElement.one() or FieldElement.zero(), depending on 
// result of FieldElement greater than or equal to comparison
class GreaterThanOrEqualToComparator extends FEComparator {

	public boolean compare(FieldElement a, FieldElement b) {
		return a.ge(b);
	}
}
