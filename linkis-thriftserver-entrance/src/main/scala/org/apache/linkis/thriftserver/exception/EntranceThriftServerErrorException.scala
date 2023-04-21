package org.apache.linkis.thriftserver.exception

import org.apache.linkis.common.exception.ErrorException

/**
 *
 * @date 2022-12-14
 * @author enjoyyin
 * @since 0.5.0
 */
class EntranceThriftServerErrorException(errCode: Int, desc: String) extends ErrorException(errCode, desc) {
}