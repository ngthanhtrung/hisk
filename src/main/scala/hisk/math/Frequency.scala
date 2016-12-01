package hisk.math

abstract class Frequency {
  def apply(numOccurrence: Int): Int
}

object Frequency {

  val Epsilon = 1e-9

  private def forceInRange(value: Int, min: Int, max: Int): Int = {
    assert(min <= max, "Minimum value must be equal to or less than maximum value.")

    if (value < min) {
      min
    } else if (value > max) {
      max
    } else {
      value
    }
  }

  def fixedOccurrence(value: Int): Frequency = {
    new Frequency {
      def apply(numOccurrence: Int): Int = {
        forceInRange(value, 0, numOccurrence)
      }
    }
  }

  def ratio(value: Double): Frequency = {
    new Frequency {
      def apply(numOccurrence: Int): Int = {
        forceInRange(Math.ceil(value * numOccurrence - Epsilon).toInt, 0, numOccurrence)
      }
    }
  }
}
