package org.learn

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by asus on 2015/7/11.
 */
object SparkStream extends App{
  val conf = new SparkConf().setAppName("SparkStream")
  //conf.setMaster("local[*]")
  conf.setMaster("spark://Master:7077").set("spark.cores.max","2")//.set("spark.executor.memory","128m")
  conf.set("spark.driver.host","192.168.94.1")//.set("spark.driver.port","50000")
  conf.setJars(List("D:\\BigdataResearch\\SparkLeaning\\out\\artifacts\\sparkleaning_jar\\sparkleaning.jar"))
  conf.set("spark.eventLog.enabled","true").set("spark.eventLog.dir","hdfs://Master:9000/tmp/spark-events")
  val ssc = new StreamingContext(conf,Seconds(1))

  // Create a DStream that will connect to hostname:port, like localhost:9999
  val lines = ssc.socketTextStream("localhost",9999)

  val words = lines.flatMap(_.split(" "))
  val pairs = words.map(x => (x,1))
  val wordCounts = pairs.reduceByKey(_ + _)
  //wordCounts.foreachRDD(println())
  //println(wordCounts)
  wordCounts.print()

  ssc.start()
  ssc.awaitTermination()

}
