package JLinAlg;

import JLinAlg.FieldElement;

// abstract class to return FieldElement.one() or FieldElement.zero(),
// depending on result of comparison
abstract class FEComparator {

	public abstract boolean compare(FieldElement a, FieldElement b);
}
