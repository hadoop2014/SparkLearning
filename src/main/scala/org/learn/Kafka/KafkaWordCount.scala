/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.learn.Kafka

import java.util.HashMap

import _root_.kafka.javaapi.consumer.ConsumerConnector
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.apache.kafka.clients.producer.{ProducerConfig, KafkaProducer, ProducerRecord}
import org.apache.spark.storage.StorageLevel

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.kafka.common.utils.Utils

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>
 *   <zkQuorum> is a list of one or more zookeeper servers that make quorum
 *   <group> is the name of kafka consumer group
 *   <topics> is a list of one or more kafka topics to consume from
 *   <numThreads> is the number of threads the kafka consumer should use
 *
 * Example:
 *    `$ bin/run-example \
 *      org.apache.spark.examples.streaming.KafkaWordCount zoo01,zoo02,zoo03 \
 *      my-consumer-group topic1,topic2 1`
 */
object KafkaWordCount {
  def main(args: Array[String]) {
    /*if (args.length < 4) {
      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }*/
    StreamingExamples.setStreamingLogLevels()

    val argsT = Array("master:2181","myGroup","word_count","1")
    val Array(zkQuorum, group, topics, numThreads) = argsT
    println(zkQuorum)
    println(group)
    println(topics)
    println(numThreads)
    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    sparkConf.setMaster("spark://Master:7077").set("spark.driver.host","192.168.94.1").set("spark.cores.max","2")
    sparkConf.set("spark.eventLog.enabled","true").set("spark.eventLog.dir","hdfs://Master:9000/tmp/spark-events")
    sparkConf.setJars(List("D:\\BigdataResearch\\SparkLearning\\out\\artifacts\\sparklearning_jar\\sparklearning.jar",
      "D:\\BigdataResearch\\SparkLearning\\out\\artifacts\\sparklearning_jar\\spark-streaming-kafka_2.10-1.4.0.jar",
      "D:\\BigdataResearch\\SparkLearning\\out\\artifacts\\sparklearning_jar\\kafka_2.10-0.8.2.1.jar",
      "D:\\BigdataResearch\\SparkLearning\\out\\artifacts\\sparklearning_jar\\zkclient-0.3.jar",
      "D:\\BigdataResearch\\SparkLearning\\out\\artifacts\\sparklearning_jar\\metrics-core-2.2.0.jar",
      "D:\\BigdataResearch\\SparkLearning\\out\\artifacts\\sparklearning_jar\\kafka-clients-0.8.2.1.jar"))

    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("hdfs://master:9000/tmp/checkpoint")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val dstream = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap,StorageLevel.MEMORY_ONLY)
    val lines = dstream.map(_._2)
    val keys = dstream.map(_._1)
    keys.foreachRDD(x => x.foreachPartition(y => y.foreach(println)))
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L))
      .reduceByKeyAndWindow(_ + _, _ - _, Seconds(10), Seconds(2), numThreads.toInt)
    wordCounts.print()
    words.foreachRDD(x => x.foreachPartition(y => y.foreach(println)))

    ssc.start()
    ssc.awaitTermination()
  }
}

// Produces some random words between 1 and 100.
object KafkaWordCountProducer {

  def main(args: Array[String]) {
    /*if (args.length < 4) {
      System.err.println("Usage: KafkaWordCountProducer <metadataBrokerList> <topic> " +
        "<messagesPerSec> <wordsPerMessage>")
      System.exit(1)
    }*/

    val argsT = Array("Master:9092","word_count","10","20")
    val Array(brokers, topic, messagesPerSec, wordsPerMessage) = argsT

    // Zookeeper connection properties
    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    // Send some messages
    while(true) {
      (1 to messagesPerSec.toInt).foreach { messageNum =>
        val str = (1 to wordsPerMessage.toInt).map(x => scala.util.Random.nextInt(10).toString)
          .mkString(" ")
        println(str)
        val message = new ProducerRecord[String, String](topic, null, str)
        producer.send(message)
      }

      Thread.sleep(4000)
    }
  }

}
