package com.spark.basic

import com.spark.study.RedisClient
import redis.clients.jedis.Jedis

object TestRedis {

  def setUp(): Unit = {

  }



  def main(args: Array[String]): Unit = {
//    val jedis = new Jedis("192.168.239.3", 6379, 1000)
//    jedis.auth("")
    val jedis = RedisClient.pool.getResource
    jedis.select(1)
    jedis.append("hello","world!")
    jedis.set("he","is a boy")

//    jedis.hset(day + "_" + camera_id, time , total + "_" + count)
    println(jedis.get("hello"))

  }
}