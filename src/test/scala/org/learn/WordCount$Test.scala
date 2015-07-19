package org.learn

import org.learn.database._
import org.learn.database.SqlResult
import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers


/**
 * Created by asus on 2015/6/27.
 */
class WordCount$Test extends FlatSpec with ShouldMatchers{
  it should  "test for spark jdbc" in {
    DatabaseConfig.spark = SparkDatabaseInfo(LoadConfig.getString("spark.host"))
    import org.learn.database.ConnectionFactoryPrefs.Spark

    Sql("drop table if exists sql_generator_test").execute
    Sql("create table sql_generator_test(value int)").execute

    val result = new SqlGenerator {
      override def genSql: String = "show tables"
    }.generateSql.query

    result.value.length should be (6)
    println(result.toString)
  }

}
