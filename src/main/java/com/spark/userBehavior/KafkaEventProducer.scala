package com.spark.userBehavior

import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

import scala.util.Random
import org.codehaus.jettison.json.JSONObject

/**
  * Created by storm on 2016/12/6.
  */
object KafkaEventProducer {
  private val users = Array(
    "4A4D769EB9679C054DE81B973ED5D768", "8dfeb5aaafc027d89349ac9a20b3930f",
    "011BBF43B89BFBF266C865DF0397AA71", "f2a8474bf7bd94f0aabbd4cdd2c06dcf",
    "068b746ed4620d25e26055a9f804385f", "97edfc08311c70143401745a03a50706",
    "d7f141563005d1b5d0d3dd30138f3f62", "c8ee90aade1671a21336c721512b817a",
    "6b67c8c700427dee7552f81f3228c927", "a95f22eabc4fd4b580c011a3161a9d9d")

  private val random = new Random()
  private var index = -1
  def getUserId(): String ={
    index = index + 1
    if (index >= users.length) index = 0
    users(index)
  }

  def click(): Double ={
    random.nextInt(10)
  }
  // bin/kafka-topics.sh --zookeeper zk1:2181,zk2:2181,zk3:2181/kafka --create --topic user_events --replication-factor 2 --partitions 2

  // bin/kafka-topics.sh --zookeeper zk1:2181,zk2:2181,zk3:2181/kafka --list

  // bin/kafka-topics.sh --zookeeper zk1:2181,zk2:2181,zk3:2181/kafka --describe user_events

  // bin/kafka-console-consumer.sh --zookeeper zk1:2181,zk2:2181,zk3:22181/kafka --topic test_json_basis_event --from-beginning
  def main(args: Array[String]) {
    val topic = "user_events"
    val brokers = "10.211.55.10:9092,10.211.55.11:9092"
    val props = new Properties()
    props.setProperty("metadata.broker.list",brokers)
    props.setProperty("serializer.class","kafka.serializer.StringEncoder")
    val kafkaConfig = new ProducerConfig(props)
    val producer = new Producer[String,String](kafkaConfig)
    while (true) {
      val event = new JSONObject()
      event
        .put("userid", getUserId())
        .put("event_time", System.currentTimeMillis().toString)
        .put("os_type", "android")
        .put("click_count", click())





      producer.send(new KeyedMessage[String, String](topic, event.toString()))
      println("message:" + event)
      Thread.sleep(200)
    }
  }
}
