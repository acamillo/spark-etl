package spark.etl

import monix.eval.Task
import org.apache.spark.sql._

final case class Reader[A](run: DataFrame => DataFrame) { self =>

  /**
   * Composes this Writer instance with another one.
   * @param that the other Reader instance to execute after this one
   * @return a new Writer as result of the two composition.
   */
  def ++[B](that: Reader[B]): Reader[A] =
    Reader(self.run andThen that.run)

  /**
   * Produce a general Spark DataFrameReader that needs configuration
   * @param f configuration function
   * @param spark an handle to Spark Session
   * @param ev evidence of Spark Encoder for data type `A`
   * @return
   */
  def reader(f: DataFrameReader => DataFrame)(
    implicit
    spark: SparkSession,
    ev: Encoder[A]
  ): DataReader[Task, A] =
    new DataReader[Task, A] {
      override def read: Task[Dataset[A]] = Task {
        f(spark.read.schema(implicitly[Encoder[A]].schema))
          .transform(self.run)
          .as[A]
      }
    }

  /**
   * Creates an instance of DataReader that expects the input to be in JSON format
   * @param paths the paths to the input files
   * @param options an option Map for Spark DataFrameReader
   * @param spark an handle to Spark Session
   * @param ev evidence of Spark Encoder for data type `A`
   * @return
   */
  def fromJson(
    paths: List[String],
    options: Map[String, String] = Map.empty
  )(implicit spark: SparkSession, ev: Encoder[A]): DataReader[Task, A] =
    reader(_.options(options).json(paths: _*))

  /**
   * Creates an instance of DataReader that expects the input to be in Parquet format
   * @param paths the paths to the input files
   * @param options an option Map for Spark DataFrameReader
   * @param spark an handle to Spark Session
   * @param ev evidence of Spark Encoder for data type `A`
   * @return
   */
  def fromParquet(
    paths: List[String],
    options: Map[String, String] = Map.empty
  )(implicit spark: SparkSession, ev: Encoder[A]): DataReader[Task, A] =
    reader(_.options(options).parquet(paths: _*))

//  def fromJson(
//    paths: List[String],
//    options: Map[String, String] = Map.empty
//  )(implicit spark: SparkSession, ev: Encoder[A]): DataReader[Task, A] =
//    new DataReader[Task, A] {
//      override def read: Task[Dataset[A]] = Task {
//        spark.read
//          .options(options)
//          .schema(implicitly[Encoder[A]].schema)
//          .json(paths: _*)
//          .transform(self.run)
//          .as[A]
//      }
//    }

//  def fromParquet(
//    paths: List[String],
//    options: Map[String, String] = Map.empty
//  )(implicit spark: SparkSession, ev: Encoder[A]): DataReader[Task, A] =
//    new DataReader[Task, A] {
//      override def read: Task[Dataset[A]] = Task {
//        spark.read
//          .options(options)
//          .schema(implicitly[Encoder[A]].schema)
//          .parquet(paths: _*)
//          .transform(self.run)
//          .as[A]
//      }
//    }

}

object Reader {
  def make[A]: Reader[A] = Reader(identity)
}
