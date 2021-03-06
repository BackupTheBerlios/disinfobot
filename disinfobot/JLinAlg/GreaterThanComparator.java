package JLinAlg;

import JLinAlg.FieldElement;

// class to return FieldElement.one() or FieldElement.zero(), depending on 
// result of FieldElement greater-than comparison
class GreaterThanComparator extends FEComparator {

	public boolean compare(FieldElement a, FieldElement b) {
		return a.gt(b);
	}
}
