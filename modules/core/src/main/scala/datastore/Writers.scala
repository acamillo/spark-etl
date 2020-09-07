package datastore

import monix.eval.Task

object Writers {
  import org.apache.spark.sql._

  type FrameWriter = DataFrameWriter[Row]

  final case class Writer[A](
    run: DataFrame => DataFrame,
    setter: FrameWriter => FrameWriter
  ) { self =>

    /**
      * Composes this Writer instance with another one.
      * @param that the other Writer instance to execute after this one
      * @return a new Writer as result of the two composition.
      */
    def +[B](that: Writer[B]): Writer[A] =
      Writer(
        run = self.run andThen that.run,
        setter = self.setter andThen that.setter
      )

    /**
      * Given a Writer it creates a DataWriter that expect a Dataset of A, applies the untyped transformation of the
      * Writer building a DataFrameWriter. It sets the DFM using the `setter` function and generate a DataWriter
      * for storing to parquet files format.
      *
      * @param destination the path to save the files
      * @return
      */
    def toParquet(destination: String): DataWriter[Task, A] =
      (ds: Dataset[A]) =>
        Task {
          setter(run(ds.toDF()).write).parquet(destination)
        }

    /**
      * Given a Writer it creates a DataWriter that expect a Dataset of A, applies the untyped transformation of the
      * Writer building a DataFrameWriter. It sets the DFM using the `setter` function and generate a DataWriter
      * for storing to CSV files format.
      *
      * @param destination the path to save the files
      * @return
      */
    def toCsv(destination: String): DataWriter[Task, A] =
      (ds: Dataset[A]) =>
        Task {
          setter(run(ds.toDF()).write).csv(destination)
        }
  }

  def mkWriter[A]: Writer[A] = Writer[A](ds => ds.toDF(), _.mode(SaveMode.Overwrite))
}
