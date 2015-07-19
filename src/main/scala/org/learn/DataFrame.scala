package org.learn

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql._

/**
 * Created by asus on 2015/5/17.
 */
object DataFrame  {
  case class SogouQ(AccessTime: String, UserID: String, QueryWord: String, HitRank: Int, HitSequence: Int, HitURL: String)
  def main(args:Array[String]) :Unit = {
    val conf = new SparkConf().setAppName("DataFrame Learning")
    conf.setMaster("spark://Master:7077").set("spark.driver.host","192.168.94.1").set("spark.cores.max","1")
    conf.set("spark.eventLog.enabled","true").set("spark.eventLog.dir","hdfs://Master:9000/tmp/spark-events")
    //conf.setMaster("local[*]")
   //conf.set("spark.executor.extraJavaOptions", "-XX:PermSize=128M -XX:MaxPermSize=256m")
    conf.setJars(List("D:\\BigdataResearch\\SparkLeaning\\out\\artifacts\\sparkleaning_jar\\sparkleaning.jar"))
    val sparkContext = new SparkContext(conf)
    //val sqlContext = new SQLContext(sparkContext)
    val hiveContext = new HiveContext(sparkContext)

    //import sqlContext.implicits._
    import hiveContext.implicits._

    println("show tables")
    //hiveContext.sql("show tables").collect().foreach(println)
    hiveContext.sql("show tables").show(10)
    //sqlContext.sql("show tables").collect().foreach(println)

    val textFile = sparkContext.textFile("hdfs://Master:9000/data/SogouQ/SogouQ.mini").map(_.split("\t"))
    val sogouQmini = textFile.map(p => SogouQ(p(0), p(1), p(2), p(3).trim.toInt, p(4).trim.toInt, p(5))).toDF
    //textFile.take(10).foreach(println)
    sogouQmini.registerTempTable("SogouQ")
    val users = sogouQmini.sqlContext.sql("select UserID,count(*) as PotNum from SogouQ group by UserID")
    //val users = sogouQmini.groupBy("UserID")
    //sogouQmini.select("UserID").collect()
    //users.take(10).foreach(println)
    //users.count().select("UserID","count").orderBy("count","UserID").take(10).foreach(println)
    //sogouQmini.groupBy("UserID").count().orderBy("count").take(10).foreach(println)
    users.show(10)
    sogouQmini.show(10)
  }
}
