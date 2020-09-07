package datastore

import org.apache.spark.sql.Dataset

import scala.language.higherKinds

trait DataReader[F[_], T] extends Serializable {
  def read: F[Dataset[T]]
}

trait DataWriter[F[_], T] extends Serializable {
  def write(ds: Dataset[T]): F[Unit]
}

trait DataReaderWriter[F[_], T] extends DataReader[F, T] with DataWriter[F, T]
