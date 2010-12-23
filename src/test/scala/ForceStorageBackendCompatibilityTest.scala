package akka.persistence.force



import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import akka.persistence.common.{QueueStorageBackendTest, VectorStorageBackendTest, MapStorageBackendTest, RefStorageBackendTest}

@RunWith(classOf[JUnitRunner])
class ForceRefStorageBackendTestIntegration extends RefStorageBackendTest   {
  def dropRefs = {
    ForceStorageBackend.refAccess.drop
  }


  def storage = ForceStorageBackend
}

@RunWith(classOf[JUnitRunner])
class ForceMapStorageBackendTestIntegration extends MapStorageBackendTest   {
  def dropMaps = {
    ForceStorageBackend.mapAccess.drop
  }


  def storage = ForceStorageBackend
}

@RunWith(classOf[JUnitRunner])
class ForceVectorStorageBackendTestIntegration extends VectorStorageBackendTest   {
  def dropVectors = {
    ForceStorageBackend.vectorAccess.drop
  }


  def storage = ForceStorageBackend
}


@RunWith(classOf[JUnitRunner])
class ForceQueueStorageBackendTestIntegration extends QueueStorageBackendTest  {
  def dropQueues = {
    ForceStorageBackend.queueAccess.drop
  }


  def storage = ForceStorageBackend
}

