package com.github.acamillo
package etl

import org.apache.spark.sql.functions.{col, input_file_name, udf}
import org.apache.spark.sql._
import spark.etl.{Reader, Transform, Writer}

/**
 * In this object I put some business logic oriented combinators.
 * When developing ETL operations it is not unusual to perform the very same
 * operations over and over again on different data-sets on the pipelines.
 *
 * The developers should not bother with Spark API but rely on a DSL
 * set of expressive, composable operators.
 *
 * These operators speak in terms on business logic making each dev immediately able to understand
 * what's their purpose and how/when to use them
 *
 */
object dsl {

  type FrameWriter = DataFrameWriter[Row]

  /**
   * It partitions the dataset into `numberOfPartitions` before writing it,
   * @param numberOfPartitions the number of partitions.
   * @return a Writer that repartition the input dataset before generating the output
   */
  def repartition(numberOfPartitions: Int): Writer[Any] =
    Writer(_.repartition(numberOfPartitions))

  /**
   * A business logic specific Writer constructor. It adds, to the output, whatever attributes it comes
   * along the input dataset. We add the db shard name and then partition the ds on it.
   * @param rhs the dataset to join and add to the transformer.
   *
   * @tparam B
   * @return
   */
  def withLocation[B](rhs: Dataset[B]): Writer[B] = {
    val addPartition: FrameWriter => FrameWriter = _.partitionBy("shard_name")
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

  /**
   * Adds an event timestamp to the dataframe. This might be useful for setting a ts for the
   * last time a dataset has been manipulated.

   * @param snapshots a map of time events keyed by account id
   * @return
   */
  def withSnapshot(snapshots: Map[Long, Long]): Writer[Any] = {
    val getSnapshotTime = udf { accountId: Long =>
      snapshots.getOrElse(accountId, 0L)
    }

    val addSnapshot: DataFrame => DataFrame =
      _.withColumn("ts_event", getSnapshotTime(col("account_id")))

    Writer(addSnapshot, identity)
  }

  /**
   * For multi tenant ETL pipeline it could possible to store accounts data into sub folders named with the account id.
   * Use this operator to fetch the account-id from the file-system structure back into the business data model.
   *
   * For example, if the source data structure is :
   * \tmp\input-data\accounts\
   *                         1234\\users\data.json
   *                         1234\\purchase\data.json
   *                         ....
   *                         5678\\users\data.json
   *                         5678\\purchase\data.json
   *                          .....
   * The prefixPath would be `\tmp\input-data\accounts`
   * @param prefixPath the common prefix
   * @return
   */
  def withAccountId(prefixPath: String): Reader[Any] = {
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
   * the predicate. For example that a date field cannot be greater than, now.
   *
   * @param predicate a function to verify whether a condition is met
   * @tparam A
   * @return
   */
  def ensure[A](predicate: A => Boolean): Transform[A, A] = Transform(_.filter(predicate))

  /**
   * In ETL is quite common to receive events out of order, for whatever order.
   * When this happen we care of just the last arrived.
   * This operator will drop row, referring to the same event, using an attribute of the entity to break ties.
   * The row are identified by the `identity` function and the `selector` function chose what field to use for picking
   * the latest value.
   *
   * For example, let's suppose we run our ETL evey hour and between two iterations we received two events saying that:
   * a NameChangeEvent event with id of 100 changed name from "John", to "Alfred.
   * Given a data type A as (id: Long, name: String, updated_at: Timestamp)
   * (100, "John", 2020-01-01 01:00:00), (100, "Alfred", "2020-01-01 01:15:00)
   *
   * Our business logic in interested in just the last value assumed by id 100. The identity is a function of
   *  field `id` and we want to pick the field with greatest `updated_at` value.
   * We use: dropLateArrival( (e:NameChangeEvent) => e.id)(_.updated_at)
   *
   * @param identity a function to uniquely identify the given data type A
   * @param select a function to pick up the entity field to break tie
   * @param o
   * @tparam T
   * @tparam A
   * @tparam K
   * @return
   */
  def dropLateArrival[T: Encoder, A, K: Encoder](
    identity: T => K
  )(select: T => A)(implicit o: Ordering[A]): Transform[T, T] =
    Transform(
      _.groupByKey(identity).reduceGroups((a: T, b: T) => if (o.compare(select(a), select(b)) > 0) a else b).map(_._2)
    )

}
