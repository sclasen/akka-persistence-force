package akka.persistence.force

import akka.persistence.common.{Backend, BytesStorage}

object ForceStorage extends BytesStorage{
  protected val backend = ForceBackend
}

object ForceBackend extends Backend[Array[Byte]]{
  val sortedSetStorage = None
  val refStorage = Some(ForceStorageBackend)
  val vectorStorage = Some(ForceStorageBackend)
  val queueStorage = Some(ForceStorageBackend)
  val mapStorage = Some(ForceStorageBackend)
}