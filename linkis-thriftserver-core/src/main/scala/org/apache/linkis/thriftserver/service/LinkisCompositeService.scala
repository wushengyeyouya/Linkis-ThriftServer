package org.apache.linkis.thriftserver.service

import java.util

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hive.service.Service.STATE
import org.apache.hive.service.{AbstractService, Service}
import org.apache.linkis.common.utils.Logging
import org.apache.linkis.thriftserver.utils.ReflectiveUtils

/**
 *
 * @date 2022-08-17
 * @author enjoyyin
 * @since 0.5.0
 */
trait LinkisCompositeService extends Logging {

  def getServices: util.Collection[Service]

  def getName: String

  def initService(hiveConf: HiveConf): Unit = {
    val iterator = this.getServices.iterator
    while (iterator.hasNext) {
      val service = iterator.next
      service.init(hiveConf)
    }
    ReflectiveUtils.invokeSetter(classOf[AbstractService], this, "ensureCurrentState", STATE.NOTINITED)
    ReflectiveUtils.invokeSetter(classOf[AbstractService], this, "changeState", STATE.INITED)
    ReflectiveUtils.writeField(classOf[AbstractService],this, "hiveConf", hiveConf)
    info(s"Service: $getName is inited.");
  }

}