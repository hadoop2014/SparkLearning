package org.learn

import scala.math.random
//import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
//import org.eclipse.jetty.servlet.api.FilterRegistration

/**
 * Created by asus on 2015/4/26.
 */
object SparkPI {
  def main(args:Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark Pi")
    conf.setMaster("spark://Master:7077").set("spark.cores.max","1")//.set("spark.executor.memory","128m")
    conf.set("spark.driver.host","192.168.94.1")//.set("spark.driver.port","50000")
    conf.setJars(List("D:\\BigdataResearch\\SparkLearning\\out\\artifacts\\sparklearning_jar\\sparklearning.jar"))
    //conf.set("spark.executor.extraJavaOptions", "-Xdebug -Xrunjdwp:transport=dt_socket,address=8005,server=y,suspend=y")
    conf.set("spark.eventLog.dir","hdfs://Master:9000/tmp/spark-events")
    conf.set("spark.eventLog.enabled","true")
    //conf.set("spark.hadoop.fs.defaultFS","hdfs://Master:9000/")
    //conf.setMaster("local")
    println("sleep begin")
    //Thread.sleep(10000)
    println("Sleep end!")

    val spark = new SparkContext(conf)
    //spark.addJar("D:\\BigdataResearch\\SparkLeaning\\out\\artifacts\\sparkleaning_jar\\sparkleaning.jar")
    val slices = if (args.length > 0)args(0).toInt else 2
    val n = 100000 * slices
    val count = spark.parallelize(1 to n, slices).map{ i =>
      val x = random * 2 -1
      val y = random * 2 -1
      if (x*x + y*y < 1) 1 else 0
    }.reduce(_ + _)
    println("Pi is roughly" + 4.0 * count / n)
    spark.stop()
  }
}
