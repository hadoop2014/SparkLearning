package org.learn

import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.SparkContext._
/**
 * Created by asus on 2015/4/26.
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount")
    conf.setMaster("spark://Master:7077").set("spark.driver.host","192.168.94.1")
    conf.setJars(List("D:\\BigdataResearch\\SparkLeaning\\out\\artifacts\\sparkleaning_jar\\sparkleaning.jar"))
    //conf.setMaster("local")
    val sc = new SparkContext(conf)
    //sc.addJar("D:/BigdataResearch/SparkLeaning/out/artifacts/sparkleaning_jar/sparklearning.jar")
    //System.setProperty("hadoop.home.dir", "D://BigdataResearch//OpenSoftware//Windows//hadoop-common-2.2.0-bin")
    if (args.length == 0) {
      System.err.println("Usage WordCount,Read default text file")
      val textFile = sc.textFile("hdfs://Master:9000/data/README.md")
      textFile.flatMap(_.split(" ")).map(x => (x,1)).reduceByKey(_ + _).map(x => (x._2,x._1)).sortByKey(false).map(x => (x._2,x._1)).take(10).foreach(println)
    }
    else {
      val textFile = sc.textFile(args(0))
      textFile.flatMap(_.split(" ")).map(x => (x,1)).reduceByKey(_ + _).map(x => (x._2,x._1)).sortByKey(false).map(x => (x._2,x._1)).take(10).foreach(println)
    }
    sc.stop()
  }
}