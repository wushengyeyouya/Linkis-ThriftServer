package org.apache.linkis.thriftserver.service.operation.handler
import java.util

/**
 *
 * @date 2022-08-30
 * @author enjoyyin
 * @since 0.5.0
 */
trait Statement {

  /**
   * 原生SQL，包含了各个[[StatementHandler]]补充后的内容。
   * 如用户提交的SQL为：
   * --set variable.a=111
   * select ${a} from default.dual
   *
   * 则该方法返回的SQL为：
   * --set variable.a=111
   * select ${a} from default.dual
   *
   * @return
   */
  def getSQL: String

  /**
   * 去掉了所有注释的精简SQL。
   * 如用户提交的SQL为：
   * --set variable.a=111
   * select ${a} from default.dual
   *
   * 则该方法返回的SQL为：
   * select ${a} from default.dual
   *
   * @return
   */
  def getSimpleSQL: String

  /**
   * 我们允许用户在SQL的注释代码之中，传递 参数变量，如以下格式：
   *
   * --set variable.a=111
   * --set startup.spark.executor.memory=2g
   * select ${a} from default.dual
   *
   * StatementHandler会解析这些注释，并将之放到properties之中，在往Linkis提交作业时，会作为对应的参数进行传递
   * @return
   */
  def getProperties: util.Map[String, String]

  def getErrorMsg: String

}

object Statement {

  val VARIABLE_PARAM = "variable."
  val RUNTIME_PARAM = "runtime."
  val STARTUP_PARAM = "startup."

  def apply(sql: String): Statement = new StatementImpl(sql, new util.HashMap[String, String], null)

  def apply(newSQL: String, statement: Statement): Statement = new StatementImpl(newSQL, statement.getSimpleSQL, statement.getProperties, statement.getErrorMsg)

  def apply(sql: String, errorMsg: String) = new StatementImpl(sql, new util.HashMap[String, String], errorMsg)

  def apply(statement: Statement) = new StatementImpl(statement.getSQL, statement.getSimpleSQL, statement.getProperties, statement.getErrorMsg)

}

class StatementImpl(sql: String, simpleSQL: String, properties: util.Map[String, String], errorMsg: String) extends Statement {

  def this(sql: String, properties: util.Map[String, String], errorMsg: String) = this(sql, sql, properties, errorMsg)

  override def getSQL: String = sql

  override def getSimpleSQL: String = simpleSQL

  override def getProperties: util.Map[String, String] = properties

  override def getErrorMsg: String = errorMsg

}
