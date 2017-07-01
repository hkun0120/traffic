package com.spark.self

import scala.util.parsing.json.{JSONArray, JSONObject}


/**
  * Created by storm on 2016/11/20.
  */
object PrintError {
  def main(args: Array[String]): Unit = {
    val site: List[String] = List("Runoob", "Google", "Baidu")
    var jsonarray = new JSONArray(site)
    var obj: Map[String, String] = Map()
    val colors = Map("red" -> "#FF0000",
      "azure" -> "#F0FFFF",
      "peru" -> "#CD853F")

    obj += ("beijing"->"China");
    obj += ("beijing"->"China1");
    obj -="beijing"
    var a =new
        JSONObject(colors )
    println(a)

    println(jsonarray)
    println(site)
    for(a <-jsonarray.list){
      println(a)
    }
  }

}
