package etl.datastore

import org.apache.spark.sql.Dataset

trait DataReader[F[_], T] extends Serializable {
  def read: F[Dataset[T]]
}
