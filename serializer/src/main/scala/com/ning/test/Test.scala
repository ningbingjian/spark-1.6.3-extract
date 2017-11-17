package com.ning.test

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * Created by zhaoshufen
  * User:  zhaoshufen
  * Date: 2017/10/28
  * Time: 18:16
  * To change this setting on:Preferences->editor->File and Code Templates->Include->File Header
  */
object Test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("test")
    conf.setMaster("local[1]")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("data/test.txt")
    val rdd1 = rdd.map{
      case line =>
        val fields = line.split("\t+")
        (fields(0),(fields(1),fields(2)))
    }
    val rdd2 =  rdd.map{
      case line =>
        val fields = line.split("\t")
        (fields(1),(fields(0),fields(2)))
    }

    val rdd3 = rdd1.join(rdd2)
      .map{
        case item=>
          (item._2._1._1,(item._2._1,item._2._2))
      }
    rdd3.collect().foreach(println)

    val rdd4 = rdd1.join(rdd3)
      .map{
        case item=>
          (item._2._1._1,(item._2._1,item._2._2))
      }
    rdd4.collect().foreach(println)

    rdd1.rightOuterJoin(rdd4)
      .collect().foreach(println)



  }

}
