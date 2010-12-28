package akka.persistence.force

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers
import akka.util.Logging
import akka.persistence.common.StorageException

class ForceBasicTestSpec extends WordSpec with MustMatchers with Logging {

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

    "fail to insert keys and values over max size" in {
      val keyLen = (255 * 3) / 4
      log.debug("Keylen %d", keyLen)
      val valLen = (32000 * 3) / 4
      log.debug("Vallen %d", valLen)
      val owner = "1"
      val keyBuf = new StringBuilder
      (1 to keyLen) foreach (i => keyBuf.append("X"))
      val valBuf = new StringBuilder
      (0 to valLen) foreach (i => valBuf.append("X"))
      val shortkey = "short"
      val shortval = "short"
      ForceStorageBackend.refAccess.getConnection
      evaluating{
        ForceStorageBackend.refAccess.put(owner, keyBuf.toString.getBytes, shortval.getBytes)
      } must produce[StorageException]

      evaluating{
        ForceStorageBackend.refAccess.put(owner, shortkey.getBytes, valBuf.toString.getBytes)
      } must produce[StorageException]

    }

  }


}