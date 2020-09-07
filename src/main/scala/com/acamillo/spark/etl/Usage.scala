package com.acamillo.spark.etl

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.input_file_name

/**
  * In this object I put some business logic oriented combinators.
  * When developing ETL operations it is not unusual to perform the very same
  * operation over and over again on different data-sets on the pipelines.
  *
  * The developers should not bother with Spark API but rely on a DSL
  * set of expressive, composable operators.
  *
  * These operators speak in terms on business logic making each dev immediatly able to understand
  * what's their purpose and how/when to use them
  *
  */
object DSL {

  import datastore.Writers._
  import datastore.Readers._
  import pipeline.Pipeline._

  import org.apache.spark.sql.functions.{ col, udf }
  import org.apache.spark.sql.{ DataFrame, Dataset, Encoder }

  /**
    * It partitions the dataset into `numberOfPartitions` before writing it,
    * @param numberOfPartitions the number of partitions.
    * @return a Writer that repartition the input dataset before generating the output
    */
  def repartition[A](numberOfPartitions: Int): Writer[A] =
    Writer(_.repartition(numberOfPartitions), identity)

  /**
    * A business logic specific Writer constructor. It adds to the output whatever attributes it comes
    * along the input dataset
    * @param rhs the dataset to join and add to the transformer.
    *
    * @tparam B
    * @return
    */
  def withLocation[B](rhs: Dataset[B]): Writer[B] = {
    val addPartition: FrameWriter => FrameWriter = _.partitionBy("write_location")
    val addLocation: DataFrame => DataFrame =
      _.join(
        rhs.withColumnRenamed("account_id", "rhs-account_id"),
        col("account_id") === col("rhs-account_id")
      ).drop("rhs-account_id")

    Writer(
      run = addLocation,
      setter = addPartition
    )
  }

  def withSnapshot[B](snapshots: Map[Long, Long]): Writer[B] = {
    val getSnapshotTime = udf { accountId: Long =>
      snapshots.getOrElse(accountId, 0L)
    }

    val addSnapshot: DataFrame => DataFrame =
      _.withColumn("snapshot_time", getSnapshotTime(col("account_id")))

    Writer(addSnapshot, identity)
  }

  /**
    * For multi tenant ETL pipeline we store accounts data into sub folders named with the account id.
    * Use this operator to fetch the account-id from the file-system structure back into the business data model.
    *
    * For example, we store the source data as:
    * \tmp\input-data\accounts\
    *                         1234\\users\data.json
    *                         1234\\purchase\data.json
    *                         ....
    *                         5678\\users\data.json
    *                         5678\\purchase\data.json
    *                          .....
    * The prefixPath would be `\tmp\input-data\accounts`
    * @param prefixPath the common prefix
    * @tparam A
    * @return
    */
  def withAccountId[A](prefixPath: String): Reader[A] = {
    val accountIdExtractor: String => Long = path =>
      path
        .substring(path.indexOf(prefixPath))
        .split("/")(prefixPath.split("/").length)
        .toLong

    val extractAccountId = udf { accountIdExtractor }

    Reader(_.withColumn("account_id", extractAccountId(input_file_name)))
  }

  /**
    * Verify that some condition are met. Specifically that some of attributes of the pipeline satisfy
    * the perdicate. For example that a date field cannot be greater than, now
    * @param predicate a function to verify whether a condition is met
    * @tparam A
    * @return
    */
  def ensure[A](predicate: A => Boolean): Transform[A, A] = Transform(_.filter(predicate))

  /**
    * In ETL is quite common to receive events in out of order. When this happen we care of just the last arrived.
    * This operator will drops row referring to the same event, using an attribute of the entity to break ties.
    * The row are identified by the `identity` function and the `selector` function chose what field to use for picking
    * the latest value.
    *
    * For example, let's suppose we run our ETL evey hour and between two deltas we received two events saying that
    * a NameChanged event with id of 100 changed name from "John", to "Alfred.
    * Given a data type A as (id: Long, name: String, updated_at: Timestamp)
    * (100, "John", 2020-01-01 01:00:00), (100, "Alfred", "2020-01-01 01:15:00)
    * Our business logic in interested in just the last value assumed by id 100. The identity is uniquely identified
    * by field `id` and the want to pick the field with greatest `updated_at` value.
    * We use: dropLateArrival( (e:NameChanged) => e.id)(_.updated_at)
    *
    * @param identity a function to uniqueli identify the given data type A
    * @param select a function to pick up the entity field to break tie
    * @param o
    * @tparam T
    * @tparam A
    * @tparam K
    * @return
    */
  def dropLateArrival[T: Encoder, A, K: Encoder](
    identity: T => K
  )(select: T => A)(implicit o: Ordering[A]): Transform[T, T] = Transform(
    _.groupByKey(identity)
      .reduceGroups((a: T, b: T) => if (o.compare(select(a), select(b)) > 0) a else b)
      .map(_._2)
  )

}

object Usage {

  import datastore.Writers._
  import datastore.Readers._
  import pipeline.Pipeline._

  import DSL._
  import org.apache.spark.sql.{ DataFrame, Dataset }
  import java.sql.Timestamp

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
  val balanceSink = mkWriter[FriendlyBalance] +
    withLocation(locations) +
    withSnapshot(Map.empty[Long, Long]) +
    repartition(10) toParquet ("/tmp/output/db/entity")

  // this pipeline will extract data from the entityReader data source. It it fails it will fall back on returning an
  // empty data set. It then adds a couple of transformations. it verify that the `id` is gt than 200
  // and then drops all the late arrivals.

  val pipeline1: Extract[User] = (fromDb(entityReader) orElse empty) >>>
    ensure(_.id != 200L) >>>
    dropLateArrival((e: User) => e.id)(_.updated_at)

  val pipeline2 = fromDb(accountReader) orElse empty

  // method to  map the pipeline result
  val toDownstream = Transform[(User, Account), FriendlyBalance](
    _.map(tuple => FriendlyBalance(tuple._1.name, tuple._2.balance))
  )

  // we join the two pipeline using the `account_id` attribute
  val p3: Extract[FriendlyBalance] = pipeline1.join(pipeline2)(_.col("account_id") === _.col("account_id")) >>> toDownstream

  // The pipeline is complete, we now store the data onto a data sink
  // will not compile as the data type do no match
  // p3 to w3
  val loader: Load = p3 to balanceSink

  loader.run()
}
