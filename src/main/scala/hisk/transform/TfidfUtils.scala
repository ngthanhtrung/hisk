package hisk.transform

import scala.collection.mutable

import hisk.math.CsrMatrix

object TfidfUtils {

  def countDocuments(termCounts: CsrMatrix[Int]): Map[Int, Int] = {
    val docCounter = mutable.Map.empty[Int, Int]

    termCounts.nonZeroValues.indices.foreach { rowIndex =>
      val rangeFromIndex = termCounts.totalNonZeroValues(rowIndex)
      val rangeToIndex = termCounts.totalNonZeroValues(rowIndex + 1)

      val rowValues = termCounts.values.slice(rangeFromIndex, rangeToIndex)
      val rowColumnIndices = termCounts.columnIndices.slice(rangeFromIndex, rangeToIndex)

      rowValues.zip(rowColumnIndices).foreach {
        case (_, termIndex) =>
          val docCount = docCounter.getOrElse(termIndex, 0)
          docCounter += termIndex -> (docCount + 1)
      }
    }

    Map(docCounter.toList: _*)
  }
}
