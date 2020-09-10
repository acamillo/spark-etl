package com.github.acamillo.sparketl

import etl.datastore.Reader
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{input_file_name, udf}

object ReaderUsage {

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
  import Reader._
  import spark.implicits._
  val jsonR = mkReader[Entity] ++ withAccountId("accounts") fromJson (List("/tmp/source/json/"))
  val pqR   = mkReader[Brand] fromParquet (List("/tmp/source/json/"))
}
