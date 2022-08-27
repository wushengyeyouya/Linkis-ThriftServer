package org.apache.hive.service.cli.thrift

import java.lang.ref.WeakReference
import java.util
import java.util.Collections

import org.apache.linkis.common.utils.Logging
import org.apache.thrift.transport.{LinkisTSaslServerTransport, TSaslServerTransport, TTransport, TTransportFactory}

import scala.collection.JavaConverters._

/**
 *
 * @date 2022-08-25
 * @author enjoyyin
 * @since 0.5.0
 */
class LinkisTTransportFactory extends TTransportFactory with Logging {

  private val transportMap: util.Map[TTransport, WeakReference[TSaslServerTransport]] =
    Collections.synchronizedMap(new util.WeakHashMap[TTransport, WeakReference[TSaslServerTransport]])

  override def getTransport(trans: TTransport): TTransport = {
    transportMap synchronized {
      val keys: util.ArrayList[TTransport] = new util.ArrayList(transportMap.keySet())
      keys.asScala.filterNot(_.isOpen).foreach { key =>
        info(s"$key is closed, remove it.")
        transportMap.remove(key)
      }
    }
    var ret = transportMap.get(trans)
    if (ret == null || ret.get == null) {
      info(s"transport map does not contain key $trans, now create a new one. Already exists ${transportMap.size()}.")
      val transport = createTSaslServerTransport(trans)
//      transport.addServerDefinition("PLAIN", AuthTypes.CUSTOM.getAuthName, null,
//        new util.HashMap[String, String], UserManagerClient.getUserManagerClient)
      transport.open()
      ret = new WeakReference[TSaslServerTransport](transport)
      transportMap.put(trans, ret)
    }
    ret.get
  }

  protected def createTSaslServerTransport(trans: TTransport): TSaslServerTransport =
    new LinkisTSaslServerTransport(trans)
}

