package JLinAlg;

import java.io.Serializable;

/**
 * This class represents an affin linear subspace.
 *
 * @author Andreas Keilhauer
 */

public class AffineLinearSubspace implements Serializable{

	protected Vector inhomogenousPart = null;
	protected Vector[] generatingSystem = null;
	protected int dimension = 0;

	/**
	 * This creates an affin linear subspace by taking an inhomogenous part and a
	 * generating System of Vectors. The subspace will be inhomogenousPart + < generatingSystem >.
	 */

	public AffineLinearSubspace(
		Vector inhomogenousPart,
		Vector[] generatingSystem)
		throws InvalidOperationException {

		if (generatingSystem != null && generatingSystem.length > 0) {
			Matrix dimensionChecker = null;
			try {
				dimensionChecker = (new Matrix(generatingSystem)).gaussjord();
			} catch (InvalidOperationException i) {
				throw new InvalidOperationException(
					"The"
						+ " generatingSystem given is unvalid."
						+ " Vectors must be of equal length and"
						+ " contain compatible FieldElement entries.");
			}
			this.dimension = dimensionChecker.rank();
			this.generatingSystem = new Vector[this.dimension];

			int index = 0;
			int index2 = 0;
			int row = 1;
			int col = 1;
			while (index < this.generatingSystem.length) {
				if (!dimensionChecker.get(row, col).isZero()) {
					this.generatingSystem[index2++] = generatingSystem[index];
					row++;
				}
				col++;
				index++;
			}
		}

		if (inhomogenousPart == null) {
			Vector tmp = generatingSystem[0];
			Vector zeroVector = new Vector(tmp.length());
			FieldElement zero = tmp.getEntry(1).zero();
			for (int i = 1; i <= zeroVector.length(); i++) {
				zeroVector.set(i, zero);
			}
			this.inhomogenousPart = zeroVector;
		} else {
			this.inhomogenousPart = inhomogenousPart;
		}
	}

	/**
	 * Gets the dimension of the affin linear subspace.
	 *
	 * @return dimension (number of independent Vectors of the generating system).
	 */

	public int getDimension() {
		return dimension;
	}

	/**
	 * Gets the inhomogenous part of this affin linear vector space.
	 * 
	 * @return inhomogenous part
	 */

	public Vector getInhomogenousPart() {
		return inhomogenousPart;
	}

	/**
	 * Gets the generating system of this affin linear vector space.
	 *
	 * @return generating system
	 */

	public Vector[] getGeneratingSystem() {
		return generatingSystem;
	}

	/**
	 * Returns a String representation of this affin linear subspace
	 *
	 * @return String representation
	 */

	public String toString() {
		String tmp = this.inhomogenousPart + " + < { ";
		for (int i = 0; i < this.dimension - 1; i++) {
			tmp += this.generatingSystem[i].toString() + ", ";
		}
		if (dimension > 0) {
			tmp += this.generatingSystem[this.dimension - 1];
		}
		return tmp + " } >";
	}
}
