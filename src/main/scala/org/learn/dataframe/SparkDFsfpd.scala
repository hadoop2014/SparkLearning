package org.learn.dataframe

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import com.databricks.spark.csv

/**
 * Created by asus on 2015/6/26.
 */

case class Sfpd(auctionid: String, bid: Float, bidtime: Float, bidder: String, bidderrate: Integer, openbid: Float, price: Float, item: String, daystolive: Integer)

object SparkDFsfpd {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkDFsfpd")
      //.setMaster("local[*]")
      .setMaster("spark://Master:7077").set("spark.driver.host", "192.168.94.1").set("spark.cores.max", "1")
      .set("spark.eventLog.enabled", "true").set("spark.eventLog.dir", "hdfs://Master:9000/tmp/spark-events")
      .set("spark.default.parallelism", "1") //.set("spark.serializer", "spark.KryoSerializer")
      .setJars(List("D:\\BigdataResearch\\SparkLeaning\\out\\artifacts\\sparkleaning_jar\\sparkleaning.jar",
        "D:\\BigdataResearch\\SparkLeaning\\out\\artifacts\\sparkleaning_jar\\spark-csv_2.10-1.0.3.jar",
        "D:\\BigdataResearch\\SparkLeaning\\out\\artifacts\\sparkleaning_jar\\commons-csv-1.1.jar"))
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._
    import sqlContext._
    import org.apache.spark.sql._

    //val df = sqlContext.load("com.databricks.spark.csv",Map("path" -> "src/main/resources/sfpd.csv","header"->"true"))
    val df = sqlContext.load("com.databricks.spark.csv",Map("path"->"hdfs://master:9000/data/sfpd.csv","header"->"true"))
    df.printSchema()
    df.select("Category").distinct.collect().foreach(println)
    df.select("Category").distinct.explain()

    df.registerTempTable("sfpd")
    sqlContext.sql("SELECT distinct Category FROM sfpd").collect().foreach(println)

    // top 10 Resolutions
    sqlContext.sql("SELECT Resolution,count(Resolution) as rescount from sfpd group by Resolution order by rescount desc limit 10").collect().foreach(println)
    val t = sqlContext.sql("SELECT Category , count(Category) as catcount FROM sfpd group by Category order by catcount desc limit 10")

    t.show()
    t.map(t => "column 0: " + t(0)).collect().foreach(println)
  }
}
