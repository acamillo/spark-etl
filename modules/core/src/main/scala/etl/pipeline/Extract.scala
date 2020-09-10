package etl.pipeline

import etl.datastore.{DataReader, DataWriter}
import monix.eval.Task
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType}
import org.apache.spark.sql.{Column, Dataset, Encoder, SparkSession}

/**
 * It models the Extraction part of an ETL pipeline
 */
final case class Extract[A](run: () => Task[Dataset[A]]) { self =>

  /**
   * Fall back operator. If this pipelines fails for any reasons it fall back to the other pipeline
   * @param that
   * @return
   */
  def orElse(that: =>Extract[A]): Extract[A] = Extract(() => run().onErrorFallbackTo(that.run()))

  /**
   * Terminates the current pipeline chain of transformation create a data sink Loader.
   *
   * @param dataWriter the data sink to use for storing the pipeline data.
   * @return
   */
  def to(dataWriter: DataWriter[Task, A]): Pipeline[Unit] = () => self.run().flatMap(dataWriter.write)

  /**
   * Convert an Extract into a Pipeline that eventually produce the count of elements
   * @return number of element in the dataset
   */
  def count: Pipeline[Long] = () => self.run().map(_.count())

  /**
   * Joins the current pipeline with another one on the columns specific at condition
   *
   * @param that the right hand side pipeline to join with
   * @param joinType the type of the join. Default join type is "inner"
   * @param condition a function to create a join condition given the two datasets*
   * @tparam B
   * @return
   */
  def join[B](that: Extract[B], joinType: JoinType = Inner)(condition: (Dataset[A], Dataset[B]) => Column): Extract[(A, B)] =
    Extract(
      () =>
        for {
          lhs <- self.run()
          rhs <- that.run()
        } yield lhs.joinWith(rhs, condition(lhs, rhs), joinType.sql)
    )

  /**
   * Mark for caching the current pipeline
   * @return
   */
  def cache: Extract[A] = Extract(() => self.run().map(_.cache()))

  /**
   * Allow sequencing of dependent pipelines
   * @param t
   * @tparam B
   * @return
   */
  def ++[B](t: Transform[A, B]): Extract[B] = Extract(() => self.run().map(t.run))

  /**
   * Handy operator for sequency dependent pipelines using legacy function transformation.
   * It is useful in case we have a set of legacy combinators based on functions.
   *
   * @param t
   * @tparam B
   * @return
   */
  def +++[B](t: Dataset[A] => Dataset[B]): Extract[B] = self ++ Transform(t)
}

object Extract {
  /**
   * Create an extract from a data source
   *
   * @param dataReader A DataReader instance
   * @tparam T the input data type
   * @return  Extract from the dataReader returning Dataset of T
   */
  def fromReader[T](dataReader: DataReader[Task, T]): Extract[T] =
    Extract(() => dataReader.read)

  /**
   * Creates an Extract from an existing Dataset[T]
   *
   * @param dataset a Spark Dataset of type T
   * @tparam T the input data type
   * @return Extract from existing dataset
   */
  def pure[T](dataset: Dataset[T]): Extract[T] = Extract(() => Task.pure(dataset))

  /**
   * Creates an empty data set of type T
   */
  def empty[T: Encoder](implicit sparkSession: SparkSession): Extract[T] =
    Extract[T](() => Task.pure(sparkSession.emptyDataset[T]))
}
