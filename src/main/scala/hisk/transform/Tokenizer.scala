package hisk.transform

import scala.util.matching.Regex

abstract class Tokenizer {
  def apply(doc: String): List[String]
}

object Tokenizer {

  def pattern(tokenPattern: Regex): Tokenizer = {
    new Tokenizer {
      def apply(doc: String): List[String] = {
        tokenPattern.findAllIn(doc).toList
      }
    }
  }
}
