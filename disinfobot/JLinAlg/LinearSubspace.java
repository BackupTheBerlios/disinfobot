package JLinAlg;

import java.io.Serializable;

/**
 * This class represents a linear subspace.
 *
 * @author Andreas Keilhauer
 */

public class LinearSubspace extends AffineLinearSubspace implements Serializable{

	/**
	 * This constructs a linear subspace with the given generating System.
	 *
	 * @param generatingSystem
	 */

	public LinearSubspace(Vector[] generatingSystem) {
		super(null, generatingSystem);
	}

	/**
	 * Returns a String representation of this linear subspace.
	 *
	 * @return String representation
	 */
	public String toString() {
		String tmp = "< { ";
		for (int i = 0; i < this.dimension - 1; i++) {
			tmp += this.generatingSystem[i].toString() + ", ";
		}
		if (dimension > 0) {
			tmp += this.generatingSystem[this.dimension - 1];
		}
		return tmp + " } >";

	}
}
