package JLinAlg;

import JLinAlg.FieldElement;

// computes minimum over elements of Vector or Matrix
class MinReduction extends Reduction {

	public void track(FieldElement currValue) {
		if (currValue.lt(reducedValue)) {
			reducedValue = currValue;
		}
	}
}
