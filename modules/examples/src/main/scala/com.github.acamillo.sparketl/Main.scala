package com.github.acamillo.sparketl

import cats.effect.ExitCode
import monix.eval.{Task, TaskApp}
import org.apache.spark.sql.Dataset


object Main extends TaskApp {

  import java.sql.Timestamp

  import dsl._
  import etl.datastore.Reader._
  import etl.datastore.Writer._
  import etl.pipeline.Extract._
  import etl.pipeline._
  import org.apache.spark.sql.SparkSession
  implicit val spark: SparkSession = ???
  import spark.implicits._

  // required for comparing java.sql.Timestamp
  implicit def orderingForTimestamps: Ordering[Timestamp] = (x: Timestamp, y: Timestamp) => x compareTo y

  case class User(id: Long, account_id: Long, name: String, updated_at: Timestamp)
  case class Account(account_id: Long, account_no: Long, balance: Long)

  // we want to show the balance per user
  case class FriendlyBalance(name: String, balance: Long)

  val accountReader = mkReader[Account] fromJson (List("/path/to/input/json/files"))
  val entityReader  = mkReader[User] fromJson (List("/path/to/input/json/files"))

  val w3 = mkWriter[User] toParquet ("/tmp/output/parquet/entity")

  val locations: Dataset[Account] = ???
  val balanceSink = mkWriter[FriendlyBalance] ++
    withLocation(locations) ++
    withSnapshot(Map.empty[Long, Long]) ++
    repartition(10) toParquet ("/tmp/output/db/entity")

  // this pipeline will extract data from the entityReader data source. It it fails it will fall back on returning an
  // empty data set. It then adds a couple of transformations. it verify that the `id` is gt than 200
  // and then drops all the late arrivals.

  val pipeline1: Extract[User] = (fromReader(entityReader) orElse empty) ++
    ensure(_.id != 200L) ++
    dropLateArrival((e: User) => e.id)(_.updated_at)

  val pipeline2 = fromReader(accountReader) orElse empty

  // method to  map the pipeline result
  val toDownstream = Transform[(User, Account), FriendlyBalance](
    _.map(tuple => FriendlyBalance(tuple._1.name, tuple._2.balance))
  )

  // we join the two pipeline using the `account_id` attribute
  val p3: Extract[FriendlyBalance] = pipeline1.join(pipeline2)(_.col("account_id") === _.col("account_id")) ++ toDownstream

  // The pipeline is complete, we now store the data onto a data sink
  // will not compile as the data type do no match
  // p3 to w3
  val loader: Pipeline[Unit] = p3 to balanceSink

  loader.run()

  // our main program
  override def run(args: List[String]): Task[ExitCode] = ???
}
