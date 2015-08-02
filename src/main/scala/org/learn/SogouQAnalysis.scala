package org.learn

import org.learn.common.LoadConfig
import org.learn.database._

import java.sql.{Connection,Statement,ResultSet}

/**
 * Created by asus on 2015/5/10.
 */
object SogouQAnalysis extends App{
  DatabaseConfig.spark = SparkDatabaseInfo(LoadConfig.getString("spark.host"))
  import org.learn.database.ConnectionFactoryPrefs.Spark
  //println(Spark.toString)
  val result = new SqlGenerator {
    override def genSql: String = "select * from sogouq limit 10"
  }.generateSql.query

  println(result.toString)

  val result1 = new SqlGenerator {
    override def genSql: String = "show tables"
  }.generateSql.query

  println(result1.toString)
}
