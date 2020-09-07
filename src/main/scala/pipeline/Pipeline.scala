package pipeline

import com.acamillo.spark.etl.DataWriter
import datastore.{DataReader, DataWriter}
import monix.eval.Task
import org.apache.spark.sql.{Column, Dataset, Encoder, SparkSession}

object Pipeline {
  type Job[A]               = Task[Dataset[A]]
  type Transformation[A, B] = Dataset[A] => Dataset[B]
  type Load                 = Pipeline[Unit]

  trait Pipeline[+A] {
    def run(): Task[A]
  }

//  /**
//    * This is the final step of an ETL. It load data into a data sink
//    * @param run
//    */
//  final case class Load(run: () => Task[Unit]) extends Pipeline [Unit] {}
//  final case class Load(run: () => Task[Unit]){ self =>
//  }

  /**
    * It models the Extraction part of an ETL pipeline
    */
  final case class Extract[A](run: () => Job[A]) { self =>

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
    def to(dataWriter: DataWriter[Task, A]): Load = () => self.run().flatMap(dataWriter.write)

    /**
      * Convert an Extract into a Pipeline that eventually produce the count of elements
      * @return number of element in the dataset
      */
    def count: Pipeline[Long] = () => self.run().map(_.count())

    /**
      * Joins the current pipeline with another one on the columns specific at condition
      * @param that the right hand side pipeline to join with
      * @param condition a function to create a join condition given the two datasets
      *
      * @tparam B
      * @return
      */
    def join[B](that: Extract[B])(condition: (Dataset[A], Dataset[B]) => Column): Extract[(A, B)] =
      Extract(
        () =>
          for {
            lhs <- self.run()
            rhs <- that.run()
          } yield lhs.joinWith(rhs, condition(lhs, rhs), "left_outer")
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
    def >>>[B](t: Transform[A, B]): Extract[B] = Extract(() => self.run().map(t.run))

    /**
      * Handy operator for sequency dependent pipelines using legacy function transformation.
      * It is useful in case we have a set of legacy combinators based on functions.
      *
      * @param t
      * @tparam B
      * @return
      */
    def +++[B](t: Transformation[A, B]): Extract[B] = self >>> Transform(t)
  }

  /**
    * This class models the concept of an ETL Transformation. It encapsulated a transforamation
    * from type A to type B.
    *
    * @param run
    * @tparam A
    * @tparam B
    */
  final case class Transform[A, B](run: Transformation[A, B]) { self =>

    /**
      * Allow sequencing of dependent transformations
      *
      * @param that the other transformation to append to this one
      *
      * @tparam C
      * @return
      */
    def +[C](that: Transform[B, C]): Transform[A, C] = Transform(ds => (self.run andThen that.run)(ds))

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
      * @param numberOfPartitions
      * @return
      */
    def repartition(numberOfPartitions: Int): Transform[A, B] = Transform { ds =>
      run(ds).repartition(numberOfPartitions)
    }
  }

  /**
    * Create an extract from a data source
    *
    * @param dataReader A DataReader instance
    * @tparam T the input data type
    * @return  Extract from the dataReader returning Dataset of T
    */
  def fromDb[T](dataReader: DataReader[Task, T]): Extract[T] =
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

// put here Transform constructors
object Transformations {
  import Pipeline._

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
