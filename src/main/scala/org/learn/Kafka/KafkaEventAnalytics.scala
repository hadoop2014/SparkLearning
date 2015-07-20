package org.learn.Kafka

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import net.sf.json.JSONObject


/**
 * Created by asus on 2015/7/16.
 */
object KafkaEventAnalytics {
  def  main (args: Array[String]) :Unit = {
    //var masterUrl = "local[1]"
    //if (args.length > 0) {
    // masterUrl = args(0)
    //}
    // Create a StreamingContext with the given master URL
    val conf = new SparkConf().setAppName("UserClickCountStat")
    conf.setMaster("spark://Master:7077").set("spark.driver.host","192.168.94.1").set("spark.cores.max","1")
    conf.set("spark.eventLog.enabled","true").set("spark.eventLog.dir","hdfs://Master:9000/tmp/spark-events")
    //conf.setMaster("local[*]")
    //conf.set("spark.executor.extraJavaOptions", "-XX:PermSize=128M -XX:MaxPermSize=256m")
    conf.setJars(List("D:\\BigdataResearch\\SparkLearning\\out\\artifacts\\sparklearning_jar\\sparklearning.jar",
      "D:\\BigdataResearch\\SparkLearning\\out\\artifacts\\sparklearning_jar\\spark-streaming-kafka_2.10-1.4.0.jar",
      "D:\\BigdataResearch\\SparkLearning\\out\\artifacts\\sparklearning_jar\\kafka_2.10-0.8.2.1.jar",
      "D:\\BigdataResearch\\SparkLearning\\out\\artifacts\\sparklearning_jar\\json-lib-2.4.jar",
      "D:\\BigdataResearch\\SparkLearning\\out\\artifacts\\sparklearning_jar\\ezmorph-1.0.6.jar"))

    val ssc = new StreamingContext(conf, Seconds(10))

    // Kafka configurations
    //val topics = Set("user_events")
    val topics = Set("word_count")
    //val topics = Map("user_events" -> 1)
    val brokers = "Master:9092"
    val kafkaParams = Map[String, String](
    "metadata.broker.list" -> brokers)//, "serializer.class" -> "kafka.serializer.StringEncoder")
    //val kafkaParams = Map(
    //  "zookeeper.connect" -> "Master:2181",
    //  "zookeeper.connection.timeout.ms" -> "10000",
    // "group.id" -> "myGroup"
    //)
    //val dbIndex = 1
    //val clickHashKey = "app::users::click"
    // Create a direct stream
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,kafkaParams,topics)
    //val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    val events = kafkaStream.flatMap(line => {
      println(line._1)
      val data = line._2
      println(data.toString)
      data
    })
    events.foreachRDD(x => x.foreachPartition(y => y.foreach(println)))
    //events.saveAsTextFiles("kafa.txt")
    /*val events = kafkaStream.flatMap(line => {
      val data = JSONObject.fromObject(line._2)
      println("receive message ,Key:" + line._1)
      //println(data)
      Some(data)
    })

    val userClicks =
      events.map(x => (x.getString("uid"),x.getInt("click_count"))).reduceByKey(_ + _)
    userClicks.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        partitionOfRecords.foreach(pair => {
          val uid = pair._1
          val clickCount = pair._2
          println(s"uid:$uid,clickCount:$clickCount")
        })
      })
    })
    */
    ssc.start()
    ssc.awaitTermination()
  }
}

