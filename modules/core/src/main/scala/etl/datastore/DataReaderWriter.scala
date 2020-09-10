package etl.datastore

import scala.language.higherKinds

trait DataReaderWriter[F[_], T] extends DataReader[F, T] with DataWriter[F, T]
