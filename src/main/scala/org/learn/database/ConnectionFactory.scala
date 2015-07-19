package org.learn.database

import java.sql.{Connection,DriverManager}
/**
 * Created by asus on 2015/5/10.
 */
trait ConnectionFactory extends LogSupport{
  def configInfo:DatabaseInfo

  override def toString: String = (s"$configInfo")

  def createConnection:Connection = {
    log.debug(s"createConnection: database config $configInfo")
    Class.forName(configInfo.driver)
    DriverManager.getConnection(configInfo.url,configInfo.user,configInfo.password)
  }

}

trait SparkConnectionFactory extends ConnectionFactory{
  override def configInfo: DatabaseInfo = DatabaseConfig.spark

  def useDatabase(conn: Connection, dbName: String) = {
    val stmt = conn.createStatement()
    log.debug("Spark useDatabase = " + s"$dbName")
    stmt.execute(s"use $dbName")
    stmt.close()
  }

  override def createConnection: Connection = {
    val conn = super.createConnection
    useDatabase(conn,configInfo.dbName)
    conn
  }
}

