package org.apache.linkis.thriftserver.service.security

import javax.security.auth.callback.{Callback, CallbackHandler, NameCallback, PasswordCallback}
import javax.security.sasl.AuthorizeCallback
import org.apache.commons.lang3.StringUtils
import org.apache.hive.service.auth.TSetIpAddressProcessor
import org.apache.linkis.common.conf.Configuration
import org.apache.linkis.common.utils.{ClassUtils, Logging}
import org.apache.linkis.thriftserver.exception.LinkisThriftServerWarnException

import scala.collection.JavaConverters._

/**
 *
 * @date 2022-08-18
 * @author enjoyyin
 * @since 0.5.0
 */
abstract class UserManagerClient extends CallbackHandler with Logging {

  override def handle(callbacks: Array[Callback]): Unit = {
    var username = ""
    var password = ""
    var ip = TSetIpAddressProcessor.getUserIpAddress
    var ac: AuthorizeCallback = null
    callbacks.foreach {
      case callback: NameCallback => username = callback.getName
      case callback: PasswordCallback => password = String.valueOf(callback.getPassword)
      case callback: AuthorizeCallback =>
        ac = callback
        if(StringUtils.isBlank(ip)) {
          info("cannot fetch ip from TSetIpAddressProcessor, now try to get it from AuthorizeCallback.")
          ip = callback.getAuthorizationID
        }
    }
    if(Configuration.IS_TEST_MODE.getValue) {
      info(s"Test mode is opened, ignore login operation. User: $username, password: $password, ip: $ip login succeed!")
    } else {
      if(StringUtils.isBlank(username)) {
        throw new LinkisThriftServerWarnException(80000, "username cannot be null.")
      } else if(StringUtils.isBlank(ip)) {
        throw new LinkisThriftServerWarnException(80000, "login failed, the ip is null, please ask admin for help.")
      }
      val pwdStr = if(StringUtils.isNotBlank(password)) "*" * password.length else "''"
      info(s"A new thrift connection called, user: $username, password: $pwdStr, ip: $ip, try to login...")
      login(username, password, ip)
    }
    if (ac != null) ac.setAuthorized(true)
  }

  def login(username: String, password: String, ipAddress: String): Unit

}

object UserManagerClient {
  private val userManagerClient = {
    val clientClasses = ClassUtils.reflections.getSubTypesOf(classOf[UserManagerClient]).asScala
      .filterNot(ClassUtils.isInterfaceOrAbstract).filterNot(_.equals(classOf[LDAPUserManagerClient])).toSeq
    if(clientClasses.isEmpty) new LDAPUserManagerClient
    else if(clientClasses.size == 1) clientClasses.head.newInstance()
    else {
      throw new Error("too much subclasses for UserManagerClient, found: " + clientClasses)
    }
  }
  def getUserManagerClient: UserManagerClient = userManagerClient
}