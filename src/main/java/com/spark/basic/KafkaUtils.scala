package com.spark.basic

/**
  * Created by storm on 2016/11/20.
  */
object KafkaUtils {
  val topics = Set("car_events")
  var brokers = "10.211.55.10:9092,10.211.55.11:9092,10.211.55.12:9092"
  val zookeeper = "10.211.55.10:2181,10.211.55.11:2181,10.211.55.12:2181"

}
