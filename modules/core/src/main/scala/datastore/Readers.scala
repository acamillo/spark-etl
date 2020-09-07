package datastore

import monix.eval.Task

object Readers {
  import org.apache.spark.sql.{SparkSession, Dataset, DataFrame, Encoder}

  final case class Reader[A](run: DataFrame => DataFrame) { self =>

    /**
      * Composes this Writer instance with another one.
      * @param that the other Reader instance to execute after this one
      * @return a new Writer as result of the two composition.
      */
    def +[B](that: Reader[B]): Reader[A] =
      Reader(self.run andThen that.run)

    def fromJson(
      paths: List[String],
      options: Map[String, String] = Map.empty
    )(implicit spark: SparkSession, ev: Encoder[A]): DataReader[Task, A] =
      new DataReader[Task, A] {
        override def read: Task[Dataset[A]] = Task {
          spark.read
            .options(options)
            .schema(implicitly[Encoder[A]].schema)
            .json(paths: _*)
            .transform(self.run)
            .as[A]
        }
      }

    def fromParquet(
      paths: List[String],
      options: Map[String, String] = Map.empty
    )(implicit spark: SparkSession, ev: Encoder[A]): DataReader[Task, A] =
      new DataReader[Task, A] {
        override def read: Task[Dataset[A]] = Task {
          spark.read
            .options(options)
            .schema(implicitly[Encoder[A]].schema)
            .parquet(paths: _*)
            .transform(self.run)
            .as[A]
        }
      }

  }

  def mkReader[A]: Reader[A] = Reader(identity)
}
