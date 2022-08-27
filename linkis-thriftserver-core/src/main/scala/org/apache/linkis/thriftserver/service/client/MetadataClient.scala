package org.apache.linkis.thriftserver.service.client

import java.util

/**
 *
 * @date 2022-08-18
 * @author enjoyyin
 * @since 0.5.0
 */
trait MetadataClient {

  def getUser: String

  def getDBS: util.List[String]

  def getTables(database: String): util.List[util.Map[String, Object]]

  def getColumns(database: String, table: String): util.List[util.Map[String, Object]]

}
