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

// TODO Move it into the example sub-project
object ReaderUsage {
  import Readers._
  import org.apache.spark.sql.functions.{input_file_name, udf}
  import org.apache.spark.sql.SparkSession

  def withAccountId[A](sourcePath: String): Reader[A] = {
    val accountIdExtractor: String => Long = path =>
      path
        .substring(path.indexOf(sourcePath))
        .split("/")(sourcePath.split("/").length)
        .toLong

    val extractAccountId = udf { accountIdExtractor }

    Reader(_.withColumn("account_id", extractAccountId(input_file_name)))
  }

  case class Entity(account_id: Long, id: Long)
  case class Brand(account_id: Long, id: Long)

  implicit val spark: SparkSession = ???
  import spark.implicits._

  val jsonR = mkReader[Entity] + withAccountId("accounts") fromJson (List("/tmp/source/json/"))
  val pqR = mkReader[Brand] fromParquet (List("/tmp/source/json/"))
}
