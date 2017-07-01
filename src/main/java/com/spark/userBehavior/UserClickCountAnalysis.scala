package com.spark.userBehavior

import kafka.serializer.StringDecoder
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}



/**
  * Created by storm on 2016/12/6.
  */
object UserClickCountAnalysis {
  def main(args: Array[String]) {
//    if (args.length ==0 ) sys.exit()
    val masterUrl = "spark://10.211.55.10:7077"
    val conf = new SparkConf().setAppName("UserClickCountAnalysis").setMaster(masterUrl).setJars(List("Users/storm/IdeaProjects/traffic/out/artifacts/Traffic_jar2/Traffic.jar"))
    val ssc = new StreamingContext(conf,Seconds(5))
    // Kafka configurations
    val topics = Set("user_events")
    val brokers = "10.211.55.10:9092,10.211.55.11:9092"
    val kafkaParams = Map[String, String](
    "metadata.broker.list" -> brokers, "serializer.class" -> "kafka.serializer.StringEncoder")


    val directKafkaStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder ](
      ssc, kafkaParams, topics)
    val events = directKafkaStream.flatMap(x=>{
      val data = JSONObject.fromObject(x._2)
//      print(data.toString())
//      println("1:"+JSONObject.fromObject(x._2).toString())
      Some(data)

    })
    val dbIndex = 1
    val clickHashKey = "app::users::click"

    val userClicks = events.map(x=>(x.getString("userid"),x.getInt("click_count"))).reduceByKey(_+_)
    userClicks.foreachRDD(partitionOfRecords=>partitionOfRecords.foreach(pair=>{
      val userid = pair._1
      val clickCount = pair._2
      val jedis = RedisClient.pool.getResource
      jedis.select(dbIndex)
      jedis.hincrBy(clickHashKey, userid, clickCount)
      RedisClient.pool.returnResource(jedis)

    }))
    ssc.start()
    ssc.awaitTermination()
  }
}
