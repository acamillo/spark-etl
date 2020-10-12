package com.github.acamillo
package etl

import java.util.Properties

import cats.effect.ExitCode
import monix.eval.{Task, TaskApp}
import org.apache.spark.sql.{DataFrameWriter, Row}
import spark.etl._

object Main extends TaskApp {

  import java.sql.Timestamp

  import dsl._
  import Extract._
  import org.apache.spark.sql.SparkSession

  implicit val spark: SparkSession = ???
  import spark.implicits._

  // required for comparing java.sql.Timestamp
  implicit def orderingForTimestamps: Ordering[Timestamp] = (x: Timestamp, y: Timestamp) => x compareTo y

  case class User(id: Long, account_id: Long, name: String, updated_at: Timestamp)
  case class Account(account_id: Long, account_no: Long, balance: Long)

  // we want to show the balance per user
  case class FriendlyBalance(name: String, balance: Long)

  val accountReader = Reader.make[Account] fromJson List("/path/to/accounts/json/files")
  val entityReader  = Reader.make[User] fromJson List("/path/to/users/json/files")

  val w3 = Writer.make[User] toParquet ("/tmp/output/parquet/entity")

  val jdbcSink: DataFrameWriter[Row] => Unit = _.jdbc("url", "table", new Properties())

  // when we store our dataset we want them to be of 10 partitions
  val balanceSink = Writer.make[FriendlyBalance] ++ repartition(10) toParquet ("/tmp/output/db/entity")

  val balanceJdbcSink = Writer.make[FriendlyBalance] writer (jdbcSink)

  // this pipeline will extract data from the entityReader data source. If it fails it falls back on returning an
  // empty data set. It then adds a couple of transformations, ie it verify that the `id` is gt than 200
  // and then drops all the late arrivals.
  val pipeline1: Extract[User] = (fromReader(entityReader) orElse empty) ++
    ensure(_.id != 200L) ++
    dropLateArrival((e: User) => e.id)(_.updated_at)

  val accounts = fromReader(accountReader) orElse empty

  // method to  map the pipeline result
  val toDownstream = Transform[(User, Account), FriendlyBalance](
    _.map(tuple => FriendlyBalance(tuple._1.name, tuple._2.balance))
  )

  // we join the two pipeline using the `account_id` attribute
  val pipeline3
    : Extract[FriendlyBalance] = pipeline1.joinWith(accounts)(_.col("account_id") === _.col("account_id")) ++ toDownstream

  // The pipeline is complete, we now store the data onto a data sink
  // the following statement will not compile as the data types do no match
  //   pipeline3 to w3

  val loader: Pipeline[Unit] = pipeline3 to balanceSink

  loader.run()

  // our main program
  override def run(args: List[String]): Task[ExitCode] = ???
}
