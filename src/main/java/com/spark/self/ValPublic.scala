package com.spark.self

import org.apache.spark.SparkConf

/**
  * Created by storm on 2016/11/20.
  */
object ValPublic {
  val conf = new SparkConf().setMaster("spark://master:7077").setJars(List("/Users/storm/IdeaProjects/traffic/out/artifacts/Traffic_jar"))
  conf.setAppName("findERRORWord")
}
