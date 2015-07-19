package org.learn.database

/**
 * Created by asus on 2015/5/10.
 */
trait SqlGenerator {
  def genSql: String

  def generateSql: Sql = Sql(genSql)
}
