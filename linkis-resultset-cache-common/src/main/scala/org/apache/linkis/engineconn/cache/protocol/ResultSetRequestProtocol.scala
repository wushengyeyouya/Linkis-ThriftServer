package org.apache.linkis.engineconn.cache.protocol

import java.util

import org.apache.linkis.protocol.Protocol
import org.apache.linkis.protocol.message.RequestProtocol

/**
  * Created by enjoyyin on 2020/10/16.
  */
trait ResultSetRequestProtocol extends RequestProtocol {
  val fsPath: String
  val pageSize: Int
}

object ResultSetRequestProtocol {
  val ENABLE_RESULT_SET_CACHE = "resultSetCacheEnable"
}

case class NextPageRequest(fsPath: String, pageSize: Int) extends ResultSetRequestProtocol

case class FirstPageRequest(fsPath: String, pageSize: Int) extends ResultSetRequestProtocol

case class FsPathListAndFirstPageRequest(parentFsPath: String, pageSize: Int)
  extends ResultSetRequestProtocol {
  override val fsPath: String = parentFsPath
}

trait ResultSetResponseProtocol extends Protocol {
  val fsPath: String
  val records: Array[Array[Any]]
  val hasNext: Boolean
  def toMap: util.HashMap[String, Object]
}

abstract class AbstractResultSetResponseProtocol extends ResultSetResponseProtocol {
  private val returnMap = new util.HashMap[String, Object]
  override def toMap: util.HashMap[String, Object] = {
    if(!returnMap.containsKey("fsPath")) {
      returnMap.put("fsPath", fsPath)
      returnMap.put("records", records)
      returnMap.put("hasNext", new java.lang.Boolean(hasNext))
    }
    returnMap
  }
  protected def addKeyValue(key: String, value: Object): Unit = returnMap.put(key, value)
}

case class ResultSetListResponse(fsPath: String, records: Array[Array[Any]], hasNext: Boolean)
  extends AbstractResultSetResponseProtocol
case class FirstPageResultSetListResponse(fsPath: String,
                                          metadata: Array[util.Map[String, String]],
                                          records: Array[Array[Any]],
                                          hasNext: Boolean) extends AbstractResultSetResponseProtocol {
  addKeyValue("metadata", metadata)
}
case class FsPathListAndFirstPageResultSetListResponse(fsPath: String,
                                                       metadata: Array[util.Map[String, String]],
                                                       records: Array[Array[Any]],
                                                       resultSetPaths: Array[String],
                                                       hasNext: Boolean) extends AbstractResultSetResponseProtocol {
  addKeyValue("metadata", metadata)
  addKeyValue("resultSetPaths", resultSetPaths)
}