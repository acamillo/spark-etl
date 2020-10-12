package spark.etl

trait DataReaderWriter[F[_], T] extends DataReader[F, T] with DataWriter[F, T]
