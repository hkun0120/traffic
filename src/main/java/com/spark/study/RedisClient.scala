package com.spark.study

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

object RedisClient extends Serializable {
  val redisHost = "10.211.55.13"
  val redisPort = 6379
  val redisTimeout = 30000
  val password = "1"
  lazy val pool = new JedisPool(new GenericObjectPoolConfig(),redisHost, redisPort, redisTimeout,password)
//  lazy val pool2 = new JedisPool(new GenericObjectPoolConfig(),redisHost,redisPort,redisTimeout,password)
  lazy val hook = new Thread {
    override def run = {
      println("Execute hook thread: " + this)
      pool.destroy()
    }
  }
  sys.addShutdownHook(hook.run)
}