package org.learn.Kafka

import java.util.Properties

import kafka.javaapi.producer.Producer
import kafka.producer.{KeyedMessage, ProducerConfig}
//import org.codehaus.jettison.json.JSONObject
import net.sf.json.JSONObject

import scala.util.Random

/**
 * Created by asus on 2015/7/13.
 */
/*
{
    "uid": "068b746ed4620d25e26055a9f804385f",
    "event_time": "1430204612405",
    "os_type": "Android",
    "click_count": 6
}
 */
object KafkaEventProducer{

  private val users = Array(
  "4A4D769EB9679C054DE81B973ED5D768", "8dfeb5aaafc027d89349ac9a20b3930f",
  "011BBF43B89BFBF266C865DF0397AA71", "f2a8474bf7bd94f0aabbd4cdd2c06dcf",
  "068b746ed4620d25e26055a9f804385f", "97edfc08311c70143401745a03a50706",
  "d7f141563005d1b5d0d3dd30138f3f62", "c8ee90aade1671a21336c721512b817a",
  "6b67c8c700427dee7552f81f3228c927", "a95f22eabc4fd4b580c011a3161a9d9d")

  private val random = new Random()
  private var pointer = -1

  def getUserID() : String = {
    pointer = pointer + 1
    if(pointer >= users.length){
      pointer = 0
      users(pointer)
    }
    else{
      users(pointer)
    }
  }

  def click() : Double ={
    random.nextInt(10)
  }

  // bin/kafka-topics.sh --zookeeper zk1:2181,zk2:2181,zk3:2181/kafka --create --topic user_events --replication-factor 2 --partitions 2
  // bin/kafka-topics.sh --zookeeper zk1:2181,zk2:2181,zk3:2181/kafka --list
  // bin/kafka-topics.sh --zookeeper zk1:2181,zk2:2181,zk3:2181/kafka --describe user_events
  // bin/kafka-console-consumer.sh --zookeeper zk1:2181,zk2:2181,zk3:22181/kafka --topic test_json_basis_event --from-beginning


  def main (args: Array[String]):Unit =  {
    val brokers = "Master:9092"
    val props = new Properties()
    val topic = "user_events"
    props.setProperty("metadata.broker.list",brokers)
    props.setProperty("serializer.class","kafka.serializer.StringEncoder")

    val kafkaConfig = new ProducerConfig(props)
    val producer = new Producer[String,String](kafkaConfig)

    while(true){
      val event = new JSONObject()
      event.element("uid",getUserID())
      .element("event_time",System.currentTimeMillis().toString)
      .element("os_type","Android")
      .element("click_count",click())
      //event.put("uid",getUserID())
      //.put("event_time",System.currentTimeMillis().toString)
      //.put("os_type","Android")
      //.put("click_count",click())

      producer.send(new KeyedMessage[String,String](topic,event.toString()))
      println("Send Message:" + event)
      Thread.sleep(200)
    }
  }
}
// Create a direct stream
