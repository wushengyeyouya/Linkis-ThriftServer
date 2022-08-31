package org.apache.linkis.thriftserver.service.operation.handler

import java.util

import org.apache.hive.service.cli.session.HiveSession
import org.apache.linkis.common.utils.{ClassUtils, Logging}
import org.apache.linkis.thriftserver.service.security.{LDAPUserManagerClient, UserManagerClient}

/**
 *
 * @date 2022-08-30
 * @author enjoyyin
 * @since 0.5.0
 */
trait StatementHandler {

  def handle(parentSession: HiveSession,
             statement: Statement, confOverlay: util.Map[String, String]): Statement

  val order: Int

}

import scala.collection.JavaConverters._
object StatementHandler extends Logging {

  private val statementHandlers: Array[StatementHandler] = {
    ClassUtils.reflections.getSubTypesOf(classOf[StatementHandler]).asScala
      .filterNot(ClassUtils.isInterfaceOrAbstract).map(_.newInstance()).toArray.sortBy(_.order)
  }
  info(s"StatementHandler list => ${statementHandlers.toList}.")

  def getStatementHandlers: Array[StatementHandler] = statementHandlers

}