package com.ning.test

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * Created by zhaoshufen
  * User:  zhaoshufen
  * Date: 2017/11/16
  * Time: 20:52
  * To change this setting on:Preferences->editor->File and Code Templates->Include->File Header
  */
case class Student(name:String,age:Int)
object App {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[1]")
    val sparkContext = new SparkContext(conf)
    val sqlContext = new SQLContext(sparkContext)
    val stu1 = Student("u1",11)
    val stu2 = Student("u2",12)
    import sqlContext.implicits._
    val rdd = sparkContext.makeRDD(Seq(stu1,stu2))
    rdd.toDF().count()
    val ds = Seq(stu1,stu2).toDS()
    ds.count()
  }
}
