package org.apache.linkis.thriftserver.service.client.remote

import java.util

import org.apache.linkis.common.utils.Logging
import org.apache.linkis.thriftserver.service.client.MetadataClient
import org.apache.linkis.ujes.client.UJESClient
import org.apache.linkis.ujes.client.request.{GetColumnsAction, GetDBSAction, GetTablesAction}

/**
 *
 * @date 2022-08-19
 * @author enjoyyin
 * @since 0.5.0
 */
class RemoteMetadataClient(userName: String, ujesClient: UJESClient) extends MetadataClient with Logging {

  override def getUser: String = userName

  override def getDBS: util.List[String] = {
    val action = GetDBSAction.builder().setUser(userName).build()
    ujesClient.getDBS(action).getDBSName()
  }

  override def getTables(database: String): util.List[util.Map[String, Object]] = {
    val action = GetTablesAction.builder().setDatabase(database).setUser(userName).build()
    ujesClient.getTables(action).getTables
  }

  override def getColumns(database: String, table: String): util.List[util.Map[String, Object]] = {
    val action = GetColumnsAction.builder().setDatabase(database).setTable(table).setUser(userName).build()
    ujesClient.getColumns(action).getColumns
  }

}
