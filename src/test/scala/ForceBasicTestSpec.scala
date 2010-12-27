package akka.persistence.force

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers

class ForceBasicTestSpec extends WordSpec with MustMatchers {

  "The ForceStorageBackend" should {
    "properly set up the correct objects, fields, tabs and permissions in a new org" in {
      ForceStorageBackend.mapAccess.drop
      ForceStorageBackend.mapAccess.getConnection
      ForceStorageBackend.vectorAccess.drop
      ForceStorageBackend.vectorAccess.getConnection
      ForceStorageBackend.queueAccess.drop
      ForceStorageBackend.queueAccess.getConnection
      ForceStorageBackend.refAccess.drop
      ForceStorageBackend.refAccess.getConnection
    }
  }
}