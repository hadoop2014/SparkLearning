package org.learn.database

import org.learn.database.Executor.QueryExecutor
import org.learn.database.Executor.CommandExecutor

/**
 * Created by asus on 2015/5/10.
 */
class Sql(private[this] val sqlStatements:String *) extends QueryExecutor with CommandExecutor{
  val sqlStatement:String = sqlStatements.mkString(";")
}

object Sql {
  def apply(sqlStatements:String *) = new Sql(sqlStatements:_*)

  implicit def toSql(statement:String):Sql = new Sql(statement)

}
