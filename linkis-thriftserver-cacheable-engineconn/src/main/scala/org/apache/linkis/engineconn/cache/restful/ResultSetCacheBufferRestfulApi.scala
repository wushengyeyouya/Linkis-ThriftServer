package org.apache.linkis.engineconn.cache.restful

import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.zip.DeflaterOutputStream

import io.protostuff.runtime.RuntimeSchema
import io.protostuff.{LinkedBuffer, ProtostuffIOUtil, Schema}
import javax.servlet.http.HttpServletResponse
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.http.entity.ContentType
import org.apache.linkis.common.utils.{ByteTimeUtils, Logging, Utils}
import org.apache.linkis.engineconn.cache.conf.ResultSetCacheConfiguration.ENGINE_RESULT_SET_CACHE_COMPRESS_SIZE
import org.apache.linkis.engineconn.cache.operation.ResultSetMetric
import org.apache.linkis.engineconn.cache.protocol.{FirstPageRequest, FsPathListAndFirstPageRequest, NextPageRequest, ResultSetResponseProtocol}
import org.apache.linkis.engineconn.cache.service.{ResultSetResponse, ResultSetService}
import org.apache.linkis.server
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation.{RequestBody, RequestMapping, RequestMethod, RestController}

/**
  * Created by enjoyyin.
  */
@RestController
@RequestMapping(path = Array("/engineconn/resultSetCacheBuffer"), produces = Array(Array("application/json")))
class ResultSetCacheBufferRestfulApi extends Logging {

  private val schemaCache = new ConcurrentHashMap[Class[_], Schema[_]]

  @Autowired
  private var resultSetService: ResultSetService = _

  def setResultSetService(resultSetService: ResultSetService): Unit = this.resultSetService = resultSetService

  private def getSchema[T](clazz: Class[T]): Schema[T] = {
    var schema = schemaCache.get(clazz).asInstanceOf[Schema[T]]
    if (schema == null) {
      schema = RuntimeSchema.getSchema(clazz)
      if (schema != null) schemaCache.put(clazz, schema)
    }
    schema
  }

  private implicit def control(serviceOp: (String, Int) => ResultSetResponse, resp: HttpServletResponse, json: util.Map[String, Object]): Unit = {
    val fsPath = json.get("fsPath").asInstanceOf[String]
    val pageSize = json.get("pageSize").asInstanceOf[Int]
    val resultSet = Utils.tryCatch(serviceOp(fsPath, pageSize)) { t =>
      resp.setStatus(400)
      val message = server.error("get resultSet failed! reason: " + ExceptionUtils.getRootCauseMessage(t), t)
      resp.setContentType(ContentType.APPLICATION_JSON.getMimeType)
      resp.getWriter.println(message)
      resp.getWriter.flush()
      return
    }
    var isCompressed = false
    ResultSetMetric.opTime{
      val buffer = LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE)
      resp.setContentType(ContentType.APPLICATION_OCTET_STREAM.getMimeType)
      val outputStream = if(resultSet.readBytes >= ENGINE_RESULT_SET_CACHE_COMPRESS_SIZE.getValue.toLong) {
        resp.setHeader("Content-Encoding", "deflate")
        isCompressed = true
        new DeflaterOutputStream(resp.getOutputStream)
      } else resp.getOutputStream
      ProtostuffIOUtil.writeTo(outputStream, resultSet.response, getSchema(resultSet.response.getClass.asInstanceOf[Class[ResultSetResponseProtocol]]), buffer)
      outputStream.close()
    } { time =>
      info(s"Costs ${ByteTimeUtils.msDurationToString(resultSet.readTime)} to iterator for ${ByteTimeUtils.bytesToString(resultSet.readBytes)} results, and ${ByteTimeUtils.msDurationToString(time)} to serialize ${if (isCompressed) "and compress " else " "}resultSets of fsPath ${resultSet.response.fsPath}.")
    }
  }

  @RequestMapping(path = Array("/nextPage"), method = Array(RequestMethod.POST))
  def getNextPage(resp: HttpServletResponse, @RequestBody json: util.Map[String, Object]): Unit = {
    logger.info(s"client fetch nextPage with: $json.")
    control((fsPath, pageSize) => resultSetService.getNextPageResponse(NextPageRequest(fsPath, pageSize)), resp, json)
  }

  @RequestMapping(path = Array("/firstPage"), method = Array(RequestMethod.POST))
  def getFirstPage(resp: HttpServletResponse, @RequestBody json: util.Map[String, Object]): Unit = {
    logger.info(s"client fetch firstPage with: $json.")
    control((fsPath, pageSize) => resultSetService.getFirstPageResponse(FirstPageRequest(fsPath, pageSize)), resp, json)
  }

  @RequestMapping(path = Array("/fsPathListAndFirstPage"), method = Array(RequestMethod.POST))
  def getFsPathListAndFirstPage(resp: HttpServletResponse, @RequestBody json: util.Map[String, Object]): Unit = {
    logger.info(s"client fetch fsPathListAndFirstPage with: $json.")
    control((fsPath, pageSize) => resultSetService.getFsPathListAndFirstPageResponse(FsPathListAndFirstPageRequest(fsPath, pageSize)), resp, json)
  }

}
