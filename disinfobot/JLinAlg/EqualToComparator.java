package JLinAlg;

import JLinAlg.FieldElement;

// class to return FieldElement.one() or FieldElement.zero(), depending on 
// result of FieldElement equal-to comparison
class EqualToComparator extends FEComparator {

	public boolean compare(FieldElement a, FieldElement b) {
		return a.equals(b);
	}
}
