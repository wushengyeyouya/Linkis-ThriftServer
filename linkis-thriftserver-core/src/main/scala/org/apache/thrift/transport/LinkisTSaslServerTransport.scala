package org.apache.thrift.transport

import java.util

import javax.security.sasl.Sasl
import org.apache.commons.lang3.StringUtils
import org.apache.hive.service.auth.HiveAuthFactory.AuthTypes
import org.apache.linkis.common.conf.Configuration
import org.apache.linkis.common.utils.Logging
import org.apache.linkis.thriftserver.service.security.UserManagerClient
import org.apache.thrift.transport.LinkisTSaslServerTransport.SEP
import org.apache.thrift.transport.TSaslTransport.NegotiationStatus

/**
 *
 * @date 2022-08-25
 * @author enjoyyin
 * @since 0.5.0
 */
class LinkisTSaslServerTransport(trans: TTransport) extends TSaslServerTransport(trans) with Logging{

  override protected def receiveSaslMessage(): TSaslTransport.SaslResponse = {
    val response = super.receiveSaslMessage()
    trans match {
      case socket: TSocket =>
        val ip = socket.getSocket.getInetAddress.getHostAddress
        var payload: Array[Byte] = if(response.payload.count(_ == SEP) == 1) SEP +: response.payload
        else if(response.payload(0) != SEP) response.payload.slice(response.payload.indexOf(SEP), response.payload.length)
        else response.payload
        payload = ip.getBytes(Configuration.BDP_ENCODING.getValue) ++ payload
        new TSaslTransport.SaslResponse(response.status, payload)
      case _ => response
    }
  }

  override protected def handleSaslStartMessage(): Unit = {
    val message = receiveSaslMessage()
    debug(s"Received start message with status ${message.status}.")
    if (message.status != NegotiationStatus.START)
      throw sendAndThrowMessage(NegotiationStatus.ERROR, "Expecting START status, received " + message.status)
    // Get the mechanism name.
    val mechanismName = new String(message.payload)
    val remoteClient = getUnderlyingTransport match {
      case socket: TSocket =>
        socket.getSocket.getInetAddress
    }
    if(StringUtils.isBlank(mechanismName) || !mechanismName.endsWith("PLAIN")) {
      warn(s"Received not supported mechanism name $mechanismName from a new remote client $remoteClient.")
      throw sendAndThrowMessage(NegotiationStatus.BAD, "Unsupported mechanism type " + mechanismName)
    }
    info(s"Received mechanism name $mechanismName from a new remote client $remoteClient.")
    setSaslServer(LinkisTSaslServerTransport.createSASLServer)
  }
}
object LinkisTSaslServerTransport {
  private def createSASLServer = Sasl.createSaslServer("PLAIN", AuthTypes.CUSTOM.getAuthName, "LinkisThriftServer",
    new util.HashMap[String, String], UserManagerClient.getUserManagerClient)
  private val SEP: Byte = 0
}