package akka.persistence.force

import akka.config.Config.config
import com.salesforce.connector.SFDCServiceConnector
import com.sforce.ws.ConnectorConfig
import org.apache.commons.codec.binary.Base64
import com.sforce.soap.metadata._
import com.sforce.soap.partner.fault.InvalidSObjectFault
import com.sforce.soap.partner.{DescribeSObjectResult, PartnerConnection}
import com.sforce.soap.partner.sobject.SObject
import akka.persistence.common.{KVStorageBackend, StorageException, CommonStorageBackendAccess, CommonStorageBackend}
import java.lang.String
import collection.mutable.{ArrayBuffer, HashSet}
import collection.immutable.{HashMap, Iterable => SIIterable}

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
  val maxOwnerKeyLength = 186
  val maxValLengthEncoded = 32000
  val maxValLength = 24000
  val base64key = new Base64(maxKeyLengthEncoded, Array.empty[Byte], true)
  val base64val = new Base64(maxValLengthEncoded, Array.empty[Byte], true)


  def getConfigOrFail(prop: String): String = {
    val value = config.getString(prop).getOrElse{
      val msg = "failed to find required property %s, blowing up".format(prop)
      log.error(msg)
      throw new IllegalStateException(msg)
    }
    if (prop == forcePasswordProp) {
      log.debug("%s -> %s".format(prop, "***was set***"))
    } else {
      log.debug("%s -> %s".format(prop, value))
    }
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
    val metaOwnerName = "owningUUID"
    val metaKeyName = "key"
    val metaValueName = "value"

    val objectName = metaObjectName + custom
    val ownerField = metaOwnerName + custom
    val keyField = metaKeyName + custom
    val valueField = metaValueName + custom
    val idField = "Name"
    val fullOwnerField = objectName + "." + ownerField
    val fullKeyField = objectName + "." + keyField
    val fullValueField = objectName + "." + valueField


    var initialized = false
    var maxWait = 30000

    val nameStandardField = new CustomField
    nameStandardField.setType(FieldType.Text)
    nameStandardField.setLabel("Name")
    val keyCustomField = new CustomField
    keyCustomField.setType(FieldType.Text)
    keyCustomField.setLabel(metaKeyName)
    keyCustomField.setFullName(fullKeyField)
    keyCustomField.setDescription("Key/External ID for akka-persistent " + store + " entries")
    keyCustomField.setExternalId(true)
    keyCustomField.setLength(maxKeyLengthEncoded)
    keyCustomField.setCaseSensitive(true)
    keyCustomField.setUnique(true)
    keyCustomField.setRequired(true)
    val ownerCustomField = new CustomField
    ownerCustomField.setType(FieldType.Text)
    ownerCustomField.setLabel(metaOwnerName)
    ownerCustomField.setFullName(fullOwnerField)
    ownerCustomField.setDescription("Owning UUID for akka-persistent " + store + " entries")
    ownerCustomField.setLength(maxKeyLengthEncoded)
    ownerCustomField.setRequired(true)
    val valueCustomField = new CustomField
    valueCustomField.setType(FieldType.LongTextArea)
    valueCustomField.setLabel(metaValueName)
    valueCustomField.setFullName(fullValueField)
    valueCustomField.setDescription("Value for akka-persistent " + store + " entries")
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
    val fieldMap = Map(keyField -> keyCustomField, ownerField -> ownerCustomField, valueField -> valueCustomField)

    val entryTab = new CustomTab
    entryTab.setCustomObject(true)
    entryTab.setFullName(objectName)
    entryTab.setMobileReady(true)
    entryTab.setMotif("Custom70: Handsaw")

    val initialQueryFragment = "select " + idField + ", " + keyField + ", " + ownerField + ", " + valueField + " from " + objectName + " where " + keyField
    val batchRetrievalSize = 200


    def drop() = {
      waitMeta(_.delete(Array(entry)))
      initialized = false
    }

    def getConnection(): PartnerConnection = {
      implicit val conn = connector.getConnection
      if (!initialized) {
        initialize
        initialized = true
      }

      conn
    }

    def initialize()(implicit conn: PartnerConnection) = {
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


      if (!conn.describeTabs.exists(res => {
        res.getTabs.exists(f => {
          f.getSobjectName == objectName
        })
      })) {
        log.debug("creating tab for %s", objectName)
        waitMeta(_.create(Array(entryTab)))
      }


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
              log.trace("deleted: owner:%s key:%s".format(sobj.getField(ownerField), sobj.getField(keyField)))
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

    override def deleteAll(owner: String, keys: SIIterable[Array[Byte]]) = {
      deleteAllSObjects(getAllSObjects(keys.map(encodeAndValidateKey(owner, _))))
    }

    def deleteAllSObjects(sobjs: Iterable[SObject]) = {
      getConnection.delete(sobjs.map(_.getField(idField).asInstanceOf[String]).toArray)
    }

    def put(owner: String, key: Array[Byte], value: Array[Byte]) = {
      val obj = createSObject(owner, key, value)
      val res = getConnection.upsert(keyField, Array(obj))
      if (!res(0).isSuccess) {
        res(0).getErrors.foreach{
          e => log.error("error during upsert:%s", e.getMessage)
        }
        throw new StorageException("error during upsert: owner: %s key:%s ".format(obj.getField(ownerField), obj.getField(keyField)))
      }
    }

    override def putAll(owner: String, keyValues: SIIterable[(Array[Byte], Array[Byte])]) = {
      val res = getConnection.upsert(keyField, keyValues.map(kv => (createSObject(owner, kv._1, kv._2))).toArray)
      res.foreach{
        result =>
          if (!result.isSuccess) {
            result.getErrors.foreach{
              e => log.error("error during upsert:%s", e.getMessage)
            }
            throw new StorageException("error during batch upsert: owner: %s ".format(owner))
          }
      }
    }

    def createSObject(owner: String, key: Array[Byte], value: Array[Byte]) = {
      val obj = new SObject
      obj.setType(objectName)
      obj.setField(keyField, encodeAndValidateKey(owner, key))
      obj.setField(valueField, encodeAndValidateValue(value))
      obj.setField(ownerField, owner)
      log.trace("upsert: owner: %s key:%s ".format(obj.getField(ownerField), obj.getField(keyField)))
      obj
    }

    def get(owner: String, key: Array[Byte], default: Array[Byte]) = {
      getSObject(owner, key) match {
        case Some(sobj) => {
          val value = sobj.getField(valueField)
          log.trace("found: owner: %s key:%s ".format(sobj.getField(ownerField), sobj.getField(keyField)))
          decodeValue(value.asInstanceOf[String])
        }
        case None => default
      }
    }

    override def getAll(owner: String, keys: SIIterable[Array[Byte]]): Map[Array[Byte], Array[Byte]] = {
      var keyMap = new HashMap[String, Array[Byte]]
      keys.foreach{
        key => {
          keyMap += encodeAndValidateKey(owner, key) -> key
        }
      }
      var result = new HashMap[Array[Byte], Array[Byte]]

      getAllSObjects(keyMap.keys).foreach{
        sobj => {
          result += keyMap(sobj.getField(keyField).asInstanceOf[String]) -> decodeValue(sobj.getField(valueField).asInstanceOf[String])
        }
      }
      result
    }

    def getSObject(owner: String, key: Array[Byte]): Option[SObject] = {
      val conn = getConnection
      val nameEnc = encodeAndValidateKey(owner, key)
      //we may get multiple records back since the SOQL is case-insensitive, so we have to do the case sensitive match here
      queryAll(getSingleQuery(nameEnc)).find(r => (r.getField(keyField) == nameEnc))
    }

    def getSingleQuery(ownerKey: String): String = {
      val query = initialQueryFragment + "='" + ownerKey + "'"
      log.trace(query)
      query
    }

    def getMultiQuery(ownerKeys: Iterable[String]): String = {
      val inKeys = ownerKeys.reduceLeft[String] {
        (acc, key) => {
          acc + "', '" + key
        }
      }
      val query = initialQueryFragment + " IN ('" + inKeys + "')"
      log.trace(query)
      query
    }

    def getMultiQueries(ownerKeys: Iterable[String]): Iterable[String] = {
      ownerKeys.sliding(batchRetrievalSize).map(getMultiQuery(_)).toIterable
    }

    def getAllSObjects(keys: Iterable[String]): ArrayBuffer[SObject] = {
      val buf = new ArrayBuffer[SObject]
      val queries = getMultiQueries(keys)
      queries foreach {
        buf ++= queryAll(_)
      }
      val filterSet = new HashSet() ++= keys
      buf.filter(sobj => (filterSet.contains(sobj.getField(keyField).asInstanceOf[String])))
    }

    def queryAll(query: String): ArrayBuffer[SObject] = {
      val conn = getConnection
      var res = conn.query(query)
      val recordList = new ArrayBuffer[SObject]
      recordList ++= res.getRecords
      while (!res.isDone) {
        res = conn.queryMore(res.getQueryLocator)
        recordList ++= res.getRecords
      }
      recordList
    }

    def encodeAndValidateKey(owner: String, key: Array[Byte]): String = {
      val keystr = base64key.encodeToString(getKey(owner, key))
      if (keystr.size > maxKeyLengthEncoded) {
        throw new StorageException("encoded owner+key was longer than %d bytes (or %d bytes unencoded)".format(maxKeyLengthEncoded, maxOwnerKeyLength))
      }
      keystr
    }

    def encodeAndValidateValue(value: Array[Byte]): String = {
      val valstr = base64val.encodeToString(value)
      if (valstr.size > maxValLengthEncoded) {
        throw new StorageException("encoded value was longer than %d bytes (or %d bytes unencoded)".format(maxValLengthEncoded, maxValLength))
      }
      log.trace("encoded->%s", valstr)
      valstr
    }

    def decodeValue(encoded: String): Array[Byte] = {
      log.trace("decode encoded->%s", encoded)
      base64val.decode(encoded)
    }


  }


}

