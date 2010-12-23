package akka.persistence.force


import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import akka.persistence.common._

@RunWith(classOf[JUnitRunner])
class ForceTicket343TestIntegration extends Ticket343Test {
  def dropMapsAndVectors: Unit = {
    ForceStorageBackend.vectorAccess.drop
    ForceStorageBackend.mapAccess.drop
  }

  def getVector: (String) => PersistentVector[Array[Byte]] = ForceStorage.getVector

  def getMap: (String) => PersistentMap[Array[Byte], Array[Byte]] = ForceStorage.getMap

}
