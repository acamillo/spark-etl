package spark.etl

import org.apache.spark.sql.Dataset

/**
 * This class models the concept of an ETL Transformation. It encapsulated a transformation
 * from a Spark Dataset[A] to a Dataset[B].
 *
 * @param run
 * @tparam A the source data type
 * @tparam B the transformed data type
 */
final case class Transform[A, B](run: Dataset[A] => Dataset[B]) { self =>

  /**
   * Allow sequencing of dependent transformations
   *
   * @param that the other transformation to append to this one
   *
   * @tparam C
   * @return
   */
  def ++[C](that: Transform[B, C]): Transform[A, C] = Transform(ds => (self.run andThen that.run)(ds))


  def flatMap[C](f: Dataset[B] => Transform[A,C]): Transform[A,C] = Transform(
    ds => f(self.run(ds)).run(ds)
  )
  /**
   * Display to console the result of the transformation
   *
   * @param truncate Whether truncate long strings. If true, strings more than 20 characters will
   *                 be truncated and all cells will be aligned right
   * @return
   */
  def show(truncate: Boolean = true): Transform[A, A] = Transform { ds =>
    run(ds).show(truncate)
    ds
  }

  /**
   * Prints the physical plan to the console for debugging purposes.
   * @return
   */
  def explain: Transform[A, A] = Transform { ds =>
    run(ds).explain()
    ds
  }

  /**
   * Returns a new Dataset that has exactly `numPartitions` partitions.
   *
   * @param numberOfPartitions the number of partitions
   * @return
   */
  def repartition(numberOfPartitions: Int): Transform[A, B] = Transform { ds =>
    run(ds).repartition(numberOfPartitions)
  }
}
