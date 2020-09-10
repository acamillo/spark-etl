package etl.pipeline

import monix.eval.Task

trait Pipeline[+A] {
  def run(): Task[A]
}
