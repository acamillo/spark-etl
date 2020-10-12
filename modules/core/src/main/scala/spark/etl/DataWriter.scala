package spark.etl

import org.apache.spark.sql.Dataset

trait DataWriter[F[_], T] extends Serializable {
  def write(ds: Dataset[T]): F[Unit]
}
