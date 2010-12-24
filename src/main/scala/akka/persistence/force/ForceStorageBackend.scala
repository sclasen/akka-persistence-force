package akka.persistence.force

import akka.config.Config.config
import com.salesforce.connector.SFDCServiceConnector
import com.sforce.ws.ConnectorConfig
import java.lang.String
import org.apache.commons.codec.binary.Base64
import com.sforce.soap.metadata._
import com.sforce.soap.partner.fault.InvalidSObjectFault
import com.sforce.soap.partner.{DescribeSObjectResult, PartnerConnection}
import com.sforce.soap.partner.sobject.SObject
import akka.persistence.common.{KVStorageBackend, StorageException, CommonStorageBackendAccess, CommonStorageBackend}
import collection.mutable.HashSet
import com.sforce.async.JobInfo

private[akka] object ForceStorageBackend extends CommonStorageBackend {

  val forceUserProp = "akka.persistence.force.username"
  val forcePasswordProp = "akka.persistence.force.password"
  val forceEndpointProp = "akka.persistence.force.endpoint"

  val forceUser = getConfigOrFail(forceUserProp)
  val forcePassword = getConfigOrFail(forcePasswordProp)
  val forceEndpoint = getConfigOrFail(forceEndpointProp) + "/services/Soap/u/20"

  val conConf = new ConnectorConfig
  conConf.setUsername(forceUser)
  conConf.setPassword(forcePassword)
  conConf.setAuthEndpoint(forceEndpoint)

  val connector = new SFDCServiceConnector("akka", conConf)

  val maxKeyLengthEncoded = 255
  val maxValLengthEncoded = 32000
  val base64key = new Base64(maxKeyLengthEncoded, Array.empty[Byte], true)
  val base64val = new Base64(maxValLengthEncoded, Array.empty[Byte], true)


  def getConfigOrFail(prop: String): String = {
    val value = config.getString(prop).getOrElse{
      val msg = "failed to find required property %s, blowing up".format(prop)
      log.error(msg)
      throw new IllegalStateException(msg)
    }
    log.debug("%s -> %s".format(prop, value))
    value
  }

  val queue = new ForceAccess("queue")

  def queueAccess = queue

  val map = new ForceAccess("map")

  def mapAccess = map

  val vector = new ForceAccess("vector")

  def vectorAccess = vector

  val ref = new ForceAccess("ref")

  def refAccess = ref


  class ForceAccess(val store: String) extends CommonStorageBackendAccess {

    import KVStorageBackend._

    val custom = "__c"
    val metaObjectName = store + "_entry"
    val metaOwnerName = "owner"
    val metaNameName = "name"
    val metaValueName = "value"

    val objectName = metaObjectName + custom
    val ownerField = metaOwnerName + custom
    val nameField = metaNameName + custom
    val valueField = metaValueName + custom
    val idField = "Name"
    val fullOwnerField = objectName + "." + metaOwnerName + custom
    val fullNameField = objectName + "." + metaNameName + custom
    val fullValueField = objectName + "." + metaValueName + custom


    var initialized = false
    var maxWait = 30000

    val nameStandardField = new CustomField
    nameStandardField.setType(FieldType.Text)
    nameStandardField.setLabel("Name")
    val nameCustomField = new CustomField
    nameCustomField.setType(FieldType.Text)
    nameCustomField.setLabel(metaNameName)
    nameCustomField.setFullName(fullNameField)
    nameCustomField.setDescription("External ID for akka-persistent " + store + " entries")
    nameCustomField.setExternalId(true)
    nameCustomField.setLength(maxKeyLengthEncoded)
    nameCustomField.setCaseSensitive(true)
    nameCustomField.setUnique(true)
    val ownerCustomField = new CustomField
    ownerCustomField.setType(FieldType.Text)
    ownerCustomField.setLabel(metaOwnerName)
    ownerCustomField.setFullName(fullOwnerField)
    ownerCustomField.setDescription("Owning UUID for akka-persistent " + store + " entries")
    ownerCustomField.setLength(maxKeyLengthEncoded)
    ownerCustomField.setCaseSensitive(true)
    val valueCustomField = new CustomField
    valueCustomField.setType(FieldType.LongTextArea)
    valueCustomField.setLabel(metaValueName)
    valueCustomField.setFullName(fullValueField)
    valueCustomField.setDescription("Owning UUID for akka-persistent " + store + " entries")
    valueCustomField.setLength(maxValLengthEncoded)
    valueCustomField.setVisibleLines(3)
    var entry: CustomObject = new CustomObject
    entry.setDeploymentStatus(DeploymentStatus.Deployed)
    entry.setSharingModel(SharingModel.ReadWrite)
    entry.setFullName(objectName)
    entry.setLabel(metaObjectName)
    entry.setPluralLabel(metaObjectName)
    entry.setNameField(nameStandardField)
    entry.setDescription("Custom object for storing akka-persistent " + store + " entries")
    val fieldMap = Map(nameField -> nameCustomField, ownerField -> ownerCustomField, valueField -> valueCustomField)

    def drop() = {
      connector.getRestConnection.createJob()
      waitMeta(_.delete(Array(entry)))
      initialized = false
    }

    def getConnection(): PartnerConnection = {
      implicit val conn = connector.getConnection
      if (!initialized) {
        var res: DescribeSObjectResult = null
        try {
          res = conn.describeSObject(objectName)
          log.debug("%s exists".format(objectName))
        } catch {
          case e: InvalidSObjectFault => {
            log.warn("%s does not exist, attempting to create", objectName)
            waitMeta(_.create(Array(entry)))
            res = conn.describeSObject(objectName)
          }
        }

        var existingFields = new HashSet[String]
        res.getFields.foreach(field => existingFields.add(field.getName))
        fieldMap.keys.filter(!existingFields.contains(_)).foreach(key => {
          log.debug("%s does not exist, creating", key)
          waitMeta(_.create(Array(fieldMap(key))))
        })

        initialized = true
      }

      conn
    }


    def waitMeta(block: MetadataConnection => Array[AsyncResult]): Unit = {
      val mconn = connector.getMetadataConnection
      val asyncRes = block(mconn)
      asyncRes.foreach{
        res =>
          var done = false
          var waited = 0
          var wait = 500
          while (!done) {
            val status = mconn.checkStatus(Array(res.getId))
            if (status(0).isDone) {
              done = true
            } else {
              Thread.sleep(wait)
              waited += wait
              wait *= 2
              if (waited > maxWait) {
                throw new StorageException("Timed out while waiting to create custom object for" + objectName)
              }
            }
          }
      }
    }



    def delete(owner: String, key: Array[Byte]) = {
      getSObject(owner, key) match {
        case Some(sobj) => {
          val deleteResults = getConnection.delete(Array(sobj.getField(idField).asInstanceOf[String]))
          deleteResults(0).getSuccess match {
            case true => {
              log.debug("deleted: owner:%s key%:s".format(sobj.getField(ownerField), sobj.getField(nameField)))
            }
            case false => {
              deleteResults(0).getErrors.foreach(e => log.error("error deleting sforceId:%s key:%s for owner:%s -> %s".format(sobj.getField(idField).asInstanceOf[String], encodeAndValidateKey(owner, key), owner, e.getMessage)))
              throw new StorageException("unable to deletekey:%s for owner:%s".format(encodeAndValidateKey(owner, key), owner))
            }
          }
        }
        case None => ()
      }
    }

    def put(owner: String, key: Array[Byte], value: Array[Byte]) = {
      val obj = new SObject
      obj.setType(objectName)
      obj.setField(nameField, encodeAndValidateKey(owner, key))
      obj.setField(valueField, encodeAndValidateValue(value))
      obj.setField(ownerField, owner)
      log.debug("upsert: owner: %s key:%s ".format(obj.getField(ownerField), obj.getField(nameField)))
      val res = getConnection.upsert(nameField, Array(obj))
      if (!res(0).isSuccess) {
        res(0).getErrors.foreach{
          e => log.error("error during upsert:%s", e.getMessage)
        }
        throw new StorageException("error during upsert: owner: %s key:%s ".format(obj.getField(ownerField), obj.getField(nameField)))
      }
    }

    def get(owner: String, key: Array[Byte], default: Array[Byte]) = {
      getSObject(owner, key) match {
        case Some(sobj) => {
          val value = sobj.getField(valueField)
          log.debug("found: owner: %s key:%s ".format(sobj.getField(ownerField), sobj.getField(nameField)))
          base64val.decode(value.asInstanceOf[String])
        }
        case None => default
      }
    }

    def getSObject(owner: String, key: Array[Byte]): Option[SObject] = {
      val nameEnc = encodeAndValidateKey(owner, key)
      val records = getConnection.query(getSingleQuery(nameEnc)).getRecords()
      records.size match {
        case 0 => None
        case _ => Some(records(0))
      //case _ => throw new StorageException("more than one record found for owner:%s nameEncoded:%s".format(owner, nameEnc))
      }
    }

    def getSingleQuery(ownerKey: String): String = {
      val query = "select " + idField + ", " + nameField + ", " + valueField + " from " + objectName + " where " + nameField + "='" + ownerKey + "'"
      log.debug(query)
      query
    }


    def encodeAndValidateKey(owner: String, key: Array[Byte]): String = {
      val keystr = base64key.encodeToString(getKey(owner, key))
      if (keystr.size > maxKeyLengthEncoded) {
        throw new IllegalArgumentException("encoded key was longer than 1024 bytes (or 768 bytes unencoded)")
      }
      keystr
    }

    def encodeAndValidateValue(value: Array[Byte]): String = {
      val keystr = base64val.encodeToString(value)
      if (keystr.size > maxValLengthEncoded) {
        throw new IllegalArgumentException("encoded key was longer than 1024 bytes (or 768 bytes unencoded)")
      }
      keystr
    }


  }


}

