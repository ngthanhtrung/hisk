package hisk.math

class CsrMatrix[A] private(
  val values: List[A],
  val columnIndices: List[Int],
  val nonZeroValues: List[Int]
) {

  def numRow: Int = nonZeroValues.size
  lazy val numColumn: Int = columnIndices.max + 1

  lazy val totalNonZeroValues: List[Int] = {
    val builder = List.newBuilder[Int]
    var totalNonZeroValue = 0

    builder += 0

    for (nonZeroValue <- nonZeroValues) {
      totalNonZeroValue += nonZeroValue
      builder += totalNonZeroValue
    }

    builder.result()
  }

  def remapColumnIndices(mapping: Map[Int, Int]): CsrMatrix[A] = {
    val remappedColumnIndices = columnIndices.map { columnIndex =>
      mapping.getOrElse(columnIndex, columnIndex)
    }

    new CsrMatrix(values, remappedColumnIndices, nonZeroValues)
  }

  def removeColumnIndices(removedColumnIndices: Set[Int]): CsrMatrix[A] = {
    val updatedValues = List.newBuilder[A]
    val updatedColumnIndices = List.newBuilder[Int]
    val updatedNonZeroValues = List.newBuilder[Int]

    nonZeroValues.indices.foreach { rowIndex =>
      val rangeFromIndex = totalNonZeroValues(rowIndex)
      val rangeToIndex = totalNonZeroValues(rowIndex + 1)

      val rowValues = values.slice(rangeFromIndex, rangeToIndex)
      val rowColumnIndices = columnIndices.slice(rangeFromIndex, rangeToIndex)

      val (updatedRowValues, updatedRowColumnIndices) = rowValues
        .zip(rowColumnIndices)
        .filterNot {
          case (_, columnIndex) =>
            removedColumnIndices.contains(columnIndex)
        }
        .unzip

      updatedValues ++= updatedRowValues
      updatedColumnIndices ++= updatedRowColumnIndices
      updatedNonZeroValues += updatedRowValues.size
    }

    new CsrMatrix(
      updatedValues.result(),
      updatedColumnIndices.result(),
      updatedNonZeroValues.result()
    )
  }
}

object CsrMatrix {

  def apply[T](
    values: List[T],
    columnIndices: List[Int],
    nonZeroValues: List[Int]
  ): CsrMatrix[T] = {
    new CsrMatrix(values, columnIndices, nonZeroValues)
  }
}
