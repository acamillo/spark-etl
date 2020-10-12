package spark.etl

import monix.eval.Task

trait Pipeline[+A] {
  def run(): Task[A]
}
