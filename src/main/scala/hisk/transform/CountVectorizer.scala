package hisk.transform

import scala.collection.mutable

import hisk.math.{CsrMatrix, Frequency}

class CountVectorizer private(
  val tokenizer: Tokenizer,
  val minNgramSize: Int,
  val maxNgramSize: Int,
  val minDf: Frequency,
  val maxDf: Frequency
) extends Transformer[List[String], CsrMatrix[Int]] {

  var vocabulary: Map[String, Int] = Map.empty

  assert(minNgramSize > 0, "Minimum n-gram size must be greater than 0.")
  assert(maxNgramSize >= minNgramSize, "Maximum n-gram size must be greater than minimum size.")

  def fit(docs: List[String]): CsrMatrix[Int] = {
    val (initialTermCounts, initialVocabulary) = countTerms(docs, fixedVocabularyOpt = None)
    val (limitedTermCounts, limitedVocabulary) = limitTerms(initialTermCounts, initialVocabulary)
    val (termCounts, vocabulary) = sortTerms(limitedTermCounts, limitedVocabulary)

    this.vocabulary = vocabulary

    termCounts
  }

  def transform(docs: List[String]): CsrMatrix[Int] = {
    val (termCounts, _) = countTerms(docs, Some(vocabulary))
    termCounts
  }

  private def preprocess(doc: String): String = {
    doc.toLowerCase
  }

  private def extractWordNgrams(tokens: List[String]): List[String] = {
    if (maxNgramSize > 1) {
      for {
        ngramSize <- (minNgramSize to maxNgramSize).toList
        fromIndex <- 0 to tokens.size - ngramSize
        ngramTokens = tokens.slice(fromIndex, fromIndex + ngramSize)
        ngram = ngramTokens.mkString(" ")
      } yield ngram

    } else {
      tokens
    }
  }

  private val extractTerms = (extractWordNgrams _)
    .compose(tokenizer.apply)
    .compose(preprocess)

  private def countTerms(
    docs: List[String],
    fixedVocabularyOpt: Option[Map[String, Int]]
  ): (CsrMatrix[Int], Map[String, Int]) = {
    val vocabulary = fixedVocabularyOpt.fold {
      mutable.Map.empty[String, Int]
    } { fixedVocabulary =>
      mutable.Map(fixedVocabulary.toList: _*)
    }

    val values = List.newBuilder[Int]
    val columnIndices = List.newBuilder[Int]
    val nonZeroCounts = List.newBuilder[Int]

    docs.foreach { doc =>
      val termCounter = mutable.Map.empty[Int, Int]
      val terms = extractTerms(doc)

      terms.foreach { term =>
        val termIndexOpt = vocabulary
          .get(term)
          .orElse {
            if (fixedVocabularyOpt.isEmpty) {
              val termIndex = vocabulary.size
              vocabulary += term -> termIndex
              Some(termIndex)
            } else {
              None
            }
          }

        termIndexOpt.foreach { termIndex =>
          val termCount = termCounter.getOrElse(termIndex, 0)
          termCounter += termIndex -> (termCount + 1)
        }
      }

      termCounter.toList.foreach {
        case (columnIndex, value) =>
          values += value
          columnIndices += columnIndex
      }

      nonZeroCounts += termCounter.size
    }

    val termCounts = CsrMatrix(
      values.result(),
      columnIndices.result(),
      nonZeroCounts.result()
    )

    val returnedVocabulary = Map(vocabulary.toList: _*)

    (termCounts, returnedVocabulary)
  }

  private def limitTerms(
    termCounts: CsrMatrix[Int],
    vocabulary: Map[String, Int]
  ): (CsrMatrix[Int], Map[String, Int]) = {
    val numDoc = termCounts.numRow
    val minDocCount = minDf(numDoc)
    val maxDocCount = maxDf(numDoc)

    val docCounts = TfidfUtils.countDocuments(termCounts)
    val removedTermIndices = docCounts
      .toList
      .flatMap {
        case (termIndex, docCount) =>
          if (docCount < minDocCount || docCount > maxDocCount) {
            Some(termIndex)
          } else {
            None
          }
      }
      .toSet

    val updatedTermCounts = termCounts.removeColumnIndices(removedTermIndices)
    val updatedVocabulary = vocabulary
      .toList
      .flatMap {
        case (term, termIndex) =>
          if (!removedTermIndices.contains(termIndex)) {
            Some(term -> termIndex)
          } else {
            None
          }
      }
      .toMap

    (updatedTermCounts, updatedVocabulary)
  }

  private def sortTerms(
    termCounts: CsrMatrix[Int],
    vocabulary: Map[String, Int]
  ): (CsrMatrix[Int], Map[String, Int]) = {
    val sortedTerms = vocabulary.toList.sortBy(_._1)
    val termIndexMapping = sortedTerms
      .zipWithIndex
      .map {
        case ((_, oldIndex), newIndex) =>
          oldIndex -> newIndex
      }
      .toMap

    val remappedTermCounts = termCounts.remapColumnIndices(termIndexMapping)
    val remappedVocabulary = vocabulary.mapValues { termIndex =>
      termIndexMapping.getOrElse(termIndex, termIndex)
    }

    (remappedTermCounts, remappedVocabulary)
  }
}

object CountVectorizer {

  def apply(
    tokenizer: Tokenizer = Tokenizer.pattern("(?U)\\b\\w\\w+\\b".r),
    minNgramSize: Int = 1,
    maxNgramSize: Int = 1,
    minDf: Frequency = Frequency.fixedOccurrence(0),
    maxDf: Frequency = Frequency.ratio(1)
  ): CountVectorizer = {
    new CountVectorizer(
      tokenizer,
      minNgramSize,
      maxNgramSize,
      minDf,
      maxDf
    )
  }
}
