package org.learn

import org.apache.spark._

/**
 * Created by asus on 2015/5/9.
 */
object PageRank {
  def main(args:Array[String]) {
    val conf = new SparkConf().setAppName("Page Rank")
    conf.setMaster("spark://Master:7077").set("spark.driver.host", "192.168.94.1").set("spark.cores.max", "1")
      .set("spark.eventLog.enabled", "true").set("spark.eventLog.dir", "hdfs://Master:9000/tmp/spark-events")
      .set("spark.default.parallelism", "1") //.set("spark.serializer", "spark.KryoSerializer")
    conf.setJars(List("D:\\BigdataResearch\\SparkLeaning\\out\\artifacts\\sparkleaning_jar\\sparkleaning.jar"))
    //conf.setMaster("local[*]")
    val context = new SparkContext(conf)

    val links = context.parallelize(Array(('A', Array('D')), ('B', Array('A')), ('C', Array('A', 'B')), ('D', Array('A', 'C'))), 2)
      .map(x => (x._1, x._2)).cache()
    var ranks = context.parallelize(Array(('A', 1.0), ('B', 1.0), ('C', 1.0), ('D', 1.0)), 2)
    val iterations_num = 50
    for (i <- 1 to iterations_num) {
      val contribs = links.join(ranks, 2).flatMap {
        case (url, (links, rank)) => links.map(dest => (dest, rank / links.size))
      }
      ranks = contribs.reduceByKey(_ + _, 2)
    }
    println("job finished!")
    ranks.foreach(println)
    println("job end!")
  }
}
