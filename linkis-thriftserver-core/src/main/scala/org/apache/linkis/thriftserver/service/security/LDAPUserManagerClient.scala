package org.apache.linkis.thriftserver.service.security
import java.util

import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hive.service.cli.HiveSQLException
import org.apache.linkis.common.utils.{LDAPUtils, Logging, Utils}

/**
 *
 * @date 2022-08-18
 * @author enjoyyin
 * @since 0.5.0
 */
class LDAPUserManagerClient extends UserManagerClient with Logging {

  override def login(username: String, password: String, ipAddress: String): Unit = {
    if(StringUtils.isBlank(password)) {
      throw new HiveSQLException("login failed, reason: password cannot be null.")
    }
    Utils.tryThrow(LDAPUtils.login(username, password)) { e =>
      val errorMsg = ExceptionUtils.getRootCauseMessage(e)
      warn(s"user $username login failed, Reason: $errorMsg", e)
      new HiveSQLException(s"Failed to open new session, Reason: $errorMsg")
    }
    info(s"user $username from ip address $ipAddress login succeed.")
  }

}
