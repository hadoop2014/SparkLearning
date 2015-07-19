package org.learn.database

/**
 * Created by asus on 2015/5/10.
 */

case class DatabaseInfo(url: String,user: String,password: String,driver: String,dbName: String)

object DatabaseConfig {
  var spark:DatabaseInfo = null
}

object SparkDatabaseInfo{
  def apply( host:String,
             port:Int = 10000,
             user:String = "",
             password:String = "",
             driver:String = "org.apache.hive.jdbc.HiveDriver",
             dbName:String = "default"
            ):DatabaseInfo = {
    DatabaseInfo(s"jdbc:hive2://$host:$port",user,password,driver,dbName)
  }
}
