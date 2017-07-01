package com.spark.study

import java.text.SimpleDateFormat
import java.util.Calendar

import kafka.serializer.StringDecoder
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

object CarEventCountAnalytics {
  def main(args: Array[String]): Unit = {
    var masterUrl = "local[1]"
    if (args.length > 0) {
      masterUrl = args(0)
    }
    System.setProperty("hadoop.home.dir", "/Users/storm/Downloads/hadoop-2.5.2");
    // Create a StreamingContext with the given master URL
    val conf = new SparkConf().setMaster(masterUrl).setAppName("UserClickCountStat")
    //创建streaming的上下文 间隔为5秒
    val ssc = new StreamingContext(conf, Seconds(5))
//    ssc.checkpoint(".")

    // Kafka configurations
    val topics = Set("car_events")
    val brokers = "10.211.55.10:9092"
      
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers, "serializer.class" -> "kafka.serializer.StringEncoder")

      
    val dbIndex = 1

    // Create a direct stream
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
//    val kafkaStream = KafkaUtils.createDirectStream(ssc, kafkaParams, fromOffsets, messageHandler)

    val events = kafkaStream.flatMap(line => {
      val data = JSONObject.fromObject(line._2)
//      println("***************"+data)
      Some(data)
    })
//            .filter(x => (x.getString("car_id").matches("'[0-9A-Z].*")))

    // Compute car average speed for every camera

//    val dateString = (input:String) => {input.substring(1,14)}
//    dateString(x.getString("event_time")) + "_" + x.getString("road_id")
    //    val ff = (a:Tuple2[Int,Int], b:Tuple2[Int,Int]) => {(a._1 + b._1, a._2 + b._2)};
    
    // (Some(data)) --> (camera_id, speed)
    val carSpeed = events.map(x => (x.getString("camera_id"),x.getInt("speed")))
//    val carSpeed = events.map(x => x.getString("road_id") -> (x.getInt("speed"),1))
//            .reduceByKey((a, b) => {(a._1 + b._1, a._2 + b._2)})
            .mapValues((x:Int)=>(x,1.toInt))
//            .reduceByKeyAndWindow((a, b) => {(a._1 + b._1, a._2 + b._2)},Seconds(10))
            
            // (camera_id,(speed,1))
            .reduceByKeyAndWindow((a:Tuple2[Int,Int], b:Tuple2[Int,Int]) => {(a._1 + b._1, a._2 + b._2)},Seconds(20),Seconds(10))

//    carSpeed.map{ case (key, value) => (key, value._1 / value._2.toFloat) }

    carSpeed.foreachRDD(rdd => {

      rdd.foreachPartition(partitionOfRecords => {
        val jedis = RedisClient.pool.getResource
        // (camera_id,(speed,1))
        partitionOfRecords.foreach(pair => {
          val camera_id = pair._1
          val total = pair._2._1
          val count = pair._2._2
          println("count:"+count)
          println("total:"+total)
          val now = Calendar.getInstance().getTime()
          // create the date/time formatters
          val minuteFormat = new SimpleDateFormat("HHmm")
          val dayFormat = new SimpleDateFormat("yyyyMMdd")
          val time = minuteFormat.format(now)
          val day = dayFormat.format(now)
          println("day:"+day)
          println("camera_id:"+camera_id)
          if(count!=0){
//            val averageSpeed = total / count
            jedis.select(dbIndex)
            jedis.hset(day + "_" + camera_id, time , total + "_" + count)
            // fetch data from redis
//            val temp = jedis.hget(day + "_" + camera_id, time)
//            println(temp)
          }
        })
        RedisClient.pool.returnResource(jedis)
      })
    })

    println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")

    ssc.start()
    ssc.awaitTermination()

  }

  /**
    * 计算在开始时间time的一段时间范围内，某条路的平均速度
    * @param time 开始时间
    * @param roadIndex 路的序号
    * @param window 时间窗口
    * @return 车辆的平均速度
    */
  def avgSpeed(time:Time,roadIndex:Int,window:Int): Float ={
    var speed = 0.0f;
    speed;
  }

  /**
    * 得到所有超速车辆的list，每个车辆的信息（时间，道路编号，车牌号）
    * @param speed 超速的阈值
    * @return List[JSONObject]
    */
  def getOverSpeedCars(speed: Int): List[JSONObject] ={
    val site: List[JSONObject] = List();

    site
  }
}