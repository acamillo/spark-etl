package spark.etl

import monix.eval.Task
import org.apache.spark.sql._

/**
 * A data type to build a chain of transformation that will eventually produce a DataWriter for a Dataset of type 'A'
 * Oftentimes when we need to perform some transformations on the data that are not part of the business logic
 * For instance, partitioning, selecting format of storage, adding processing time etc
 * We want these operation to be part of the last step of the ETL process, when the Load part is invoked.
 *
 * @param run The transformation to be applied to the incoming Dataset before it is stored
 * @param setter sets the DataFrameWriter as required
 * @param mode the save mode. eg: Overwrite, Append, Ignore, etc
 * @tparam A
 */
final case class Writer[A](
  run: DataFrame => DataFrame,
  setter: DataFrameWriter[Row] => DataFrameWriter[Row] = identity,
  mode: SaveMode = SaveMode.Overwrite
) { self =>

  /**
   * Composes this Writer instance with another one. It preserves the 'mode' of the very first Writer instance
   *
   * @param that the other Writer instance to execute after this one
   * @return a new Writer as result of the two composition.
   */
  def ++(that: Writer[Any]): Writer[A] =
    Writer(
      run = self.run andThen that.run,
      setter = self.setter andThen that.setter,
      mode = self.mode
    )

  /**
   * Given a Writer it creates a DataWriter that expect a Dataset of A, applies the untyped transformation of the
   * Writer building a DataFrameWriter. It sets the DFM using the `setter` function and generate a DataWriter
   * for storing to parquet files format.
   *
   * @param destination the path to save the files
   * @return
   */
  def toParquet(destination: String): DataWriter[Task, A] = writer(_.parquet(destination))

  /**
   * Given a Writer it creates a DataWriter that expect a Dataset of A, applies the untyped transformation of the
   * Writer building a DataFrameWriter. It sets the DFM using the `setter` function and generate a DataWriter
   * for storing to CSV files format.
   *
   * @param destination the path to save the files
   * @return
   */
  def toCsv(destination: String,
            options: Map[String, String] = Map.empty): DataWriter[Task, A] =
    writer(_.options(options).csv(destination))

  /**
   * Creates a generic DataWriter
   * @param f
   * @return
   */
  def writer(f: DataFrameWriter[Row] => Unit): DataWriter[Task, A] =
    (ds: Dataset[A]) =>
      Task {
        f(setter(run(ds.toDF()).write).mode(mode))
      }
}

object Writer {
  import org.apache.spark.sql._

  def make[A]: Writer[A]                 = Writer[A](ds => ds.toDF(), mode = SaveMode.Overwrite)
  def make[A](mode: SaveMode): Writer[A] = Writer[A](ds => ds.toDF(), mode = mode)
}
