package hisk.naivebayes

import hisk.math.CsrMatrix

class MultinomialNaiveBayes private(
  alpha: Double,
  fitPrior: Boolean
) {

  var labelMapping = Map.empty[String, Int]

  var classLogPriors = Map.empty[Int, Double]
  var featureLogProbabilities = Array.ofDim[Double](0, 0)

  def fit(data: CsrMatrix[Double], labels: List[String]): Unit = {
    labelMapping = labels
      .distinct
      .zipWithIndex
      .toMap

    val numFeature = data.numColumn
    val numClass = labelMapping.size

    val featureCounts = Array.fill[Double](numClass, numFeature)(alpha)
    val classFeatureCounts = Array.fill[Double](numClass)(alpha * numFeature)

    data.nonZeroValues.indices.foreach { rowIndex =>
      val rangeFromIndex = data.totalNonZeroValues(rowIndex)
      val rangeToIndex = data.totalNonZeroValues(rowIndex + 1)

      val rowValues = data.values.slice(rangeFromIndex, rangeToIndex)
      val rowColumnIndices = data.columnIndices.slice(rangeFromIndex, rangeToIndex)

      rowValues.zip(rowColumnIndices).foreach {
        case (value, feature) =>
          val clazz = labelMapping(labels(rowIndex))
          featureCounts(clazz)(feature) += value
          classFeatureCounts(clazz) += value
      }
    }

    featureLogProbabilities = Array.ofDim(numClass, numFeature)

    for (clazz <- 0 until numClass; feature <- 0 until numFeature) {
      featureLogProbabilities(clazz)(feature) += Math.log(featureCounts(clazz)(feature))
      featureLogProbabilities(clazz)(feature) -= Math.log(classFeatureCounts(clazz))
    }

    if (fitPrior) {
      val classCounts = labels
        .groupBy(labelMapping(_))
        .mapValues(_.size)

      val totalClassCount = classCounts.values.sum

      classLogPriors = classCounts.map {
        case (clazz, classCount) =>
          val prior = Math.log(classCount) - Math.log(totalClassCount)
          clazz -> prior
      }

    } else {
      classLogPriors = labelMapping
        .values
        .map { clazz =>
          clazz -> -Math.log(labelMapping.size)
        }
        .toMap
    }
  }

  def predict(data: CsrMatrix[Double]): List[String] = {
    val labels = List.newBuilder[String]

    data.nonZeroValues.indices.foreach { rowIndex =>
      val rangeFromIndex = data.totalNonZeroValues(rowIndex)
      val rangeToIndex = data.totalNonZeroValues(rowIndex + 1)

      val rowValues = data.values.slice(rangeFromIndex, rangeToIndex)
      val rowColumnIndices = data.columnIndices.slice(rangeFromIndex, rangeToIndex)

      var bestLabel = labelMapping.keys.head
      var bestProbability = Double.MinValue

      labelMapping.foreach {
        case (label, clazz) =>
          var probability = classLogPriors(clazz)

          rowValues.zip(rowColumnIndices).foreach {
            case (value, feature) =>
              probability += value * featureLogProbabilities(clazz)(feature)
          }

          if (probability > bestProbability) {
            bestLabel = label
            bestProbability = probability
          }
      }

      labels += bestLabel
    }

    labels.result()
  }
}

object MultinomialNaiveBayes {

  def apply(
    alpha: Double = 1d,
    fitPrior: Boolean = true
  ): MultinomialNaiveBayes = {
    new MultinomialNaiveBayes(alpha, fitPrior)
  }
}
