package com.spark.userBehavior

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

/**
  * Created by storm on 2016/12/7.
  */
object RedisClient extends Serializable{
  val redisHost = "10.211.55.13"
  val redisPort = 6379
  val redisTimeout = 30000
  lazy val pool = new JedisPool(new GenericObjectPoolConfig(), redisHost, redisPort, redisTimeout)
  lazy val hook = new Thread {
    override def run = {
      println("Execute hook thread: " + this)
      pool.destroy()
    }

  }
  sys.addShutdownHook(hook.run)

}
