package etl.pipeline

import org.apache.spark.sql.Dataset

/**
  * This class models the concept of an ETL Transformation. It encapsulated a transforamation
  * from type A to type B.
  *
  * @param run
  * @tparam A
  * @tparam B
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

// put here Transform constructors
object Transform {

  import org.apache.spark.sql.Encoder

  /**
    * Constructor for a chain of transformation
    *
    * @tparam A
    * @return
    */
  def beginTransform[A]: Transform[A, A] = Transform(ds => ds)

  /**
    * Creates a Transform that filters all pipeline element according to the given predicate
    *
    * @param predicate a function to decide what element to filter
    * @tparam A the input data type
    * @return
    */
  def ensure[A](predicate: A => Boolean): Transform[A, A] = Transform(_.filter(predicate))

  def collectWith[A, B](pf: PartialFunction[A, B])(implicit enc: Encoder[B]): Transform[A, B] = Transform(
    _.filter(pf.isDefinedAt _).map(pf)
  )

  def dropLateArrival[T: Encoder, A, K: Encoder](
    identity: T => K
  )(select: T => A)(implicit o: Ordering[A]): Transform[T, T] = Transform(
    _.groupByKey(identity)
      .reduceGroups((a: T, b: T) => if (o.compare(select(a), select(b)) > 0) a else b)
      .map(_._2)
  )
}
