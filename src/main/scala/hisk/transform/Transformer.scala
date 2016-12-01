package hisk.transform

abstract class Transformer[I, O] {
  def fit(input: I): O
  def transform(input: I): O
}
