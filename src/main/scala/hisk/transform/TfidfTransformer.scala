package hisk.transform

import hisk.math.CsrMatrix

class TfidfTransformer private(
  val useIdf: Boolean,
  val smoothIdf: Boolean,
  val sublinearTf: Boolean
) extends Transformer[CsrMatrix[Int], CsrMatrix[Double]] {

  var idf: Map[Int, Double] = Map.empty

  def fit(termCounts: CsrMatrix[Int]): CsrMatrix[Double] = {
    if (useIdf) {
      val numDoc = termCounts.numRow

      idf = TfidfUtils
        .countDocuments(termCounts)
        .mapValues { docCount =>
          if (smoothIdf) {
            Math.log((numDoc.toDouble + 1) / (docCount + 1)) + 1
          } else {
            Math.log(numDoc.toDouble / docCount) + 1
          }
        }
    }

    transform(termCounts)
  }

  def transform(termCounts: CsrMatrix[Int]): CsrMatrix[Double] = {
    val updatedValues = List.newBuilder[Double]

    termCounts.nonZeroValues.indices.foreach { rowIndex =>
      val rangeFromIndex = termCounts.totalNonZeroValues(rowIndex)
      val rangeToIndex = termCounts.totalNonZeroValues(rowIndex + 1)

      val rowValues = termCounts.values.slice(rangeFromIndex, rangeToIndex)
      val rowColumnIndices = termCounts.columnIndices.slice(rangeFromIndex, rangeToIndex)

      val rowTfidfs = rowValues.zip(rowColumnIndices).map {
        case (termCount, termIndex) =>
          val tf = if (sublinearTf) {
            Math.log(termCount) + 1
          } else {
            termCount
          }

          if (useIdf) {
            tf * idf(termIndex)
          } else {
            tf
          }
      }

      val l2Sum = rowTfidfs.foldLeft(0d) { (l2Sum, tfidf) =>
        l2Sum + tfidf * tfidf
      }

      val sqrtL2Sum = Math.sqrt(l2Sum)
      val normalizedRowTfIdfs = rowTfidfs.map(_ / sqrtL2Sum)

      updatedValues ++= normalizedRowTfIdfs
    }

    CsrMatrix(
      updatedValues.result(),
      termCounts.columnIndices,
      termCounts.nonZeroValues
    )
  }
}

object TfidfTransformer {

  def apply(
    useIdf: Boolean = true,
    smoothIdf: Boolean = true,
    sublinearTf: Boolean = false
  ): TfidfTransformer = {
    new TfidfTransformer(useIdf, smoothIdf, sublinearTf)
  }
}
