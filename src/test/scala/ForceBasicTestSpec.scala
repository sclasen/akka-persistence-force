package akka.persistence.force

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers

class ForceBasicTestSpec extends WordSpec with MustMatchers {

  "The ForceStorageBackend" should {
    "load properly when valid configuration is used" in {
      ForceStorageBackend.queueAccess must not be (null)
    }

    "insert or update an entry" in {
      ForceStorageBackend.queueAccess.put("owner", "test".getBytes, "testval".getBytes)
    }

    "retrieve an entry" in {
      ForceStorageBackend.queueAccess.put("owner", "test".getBytes, "testval".getBytes)
      val returned = ForceStorageBackend.queueAccess.get("owner", "test".getBytes)
      "testval" must be(new String(returned))
    }

    "delete an entry" in {
      ForceStorageBackend.queueAccess.put("owner", "test".getBytes, "testval".getBytes)
      val returned = ForceStorageBackend.queueAccess.get("owner", "test".getBytes)
      "testval" must be(new String(returned))
      ForceStorageBackend.queueAccess.delete("owner", "test".getBytes)
      ForceStorageBackend.queueAccess.get("owner", "test".getBytes, "default".getBytes) must be("default".getBytes)
    }

    "drop all " in {
      ForceStorageBackend.mapAccess.getConnection
      ForceStorageBackend.mapAccess.drop
    }

    "test" in {
      val enc = ForceStorageBackend.mapAccess.encodeIndexedKey("test", 1234)
      ForceStorageBackend.mapAccess.decodeIndexedKey("test", enc) must be(1234)

      val encm = ForceStorageBackend.mapAccess.encodeMapKey("test", "mapKey12341234".getBytes)
      ForceStorageBackend.mapAccess.decodeMapKey("test", encm) must be("mapKey12341234".getBytes)
    }
  }
}