package JLinAlg;

import JLinAlg.FieldElement;


// absolute value computation for FieldElement
class AbsOperator implements MonadicOperator {

    public FieldElement apply(FieldElement x) {
	return x.lt(x.zero()) ? x.zero().subtract(x) : x;
    }

}
