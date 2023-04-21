package org.apache.linkis.thriftserver

import java.util

import org.apache.linkis.rpc.Sender
import org.apache.linkis.thriftserver.service.client.MetadataClient

/**
 *
 * @date 2023-02-09
 * @author enjoyyin
 * @since 0.5.0
 */
class EntranceMetadataClient(user: String) extends MetadataClient {

  override def getUser: String = user

  override def getDBS: util.List[String] = {

  }

  override def getTables(database: String): util.List[util.Map[String, Object]] = ???

  override def getColumns(database: String, table: String): util.List[util.Map[String, Object]] = ???
}
object EntranceMetadataClient {
  private val sender = Sender.getSender("")
}