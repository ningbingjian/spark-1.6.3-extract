package com.ning.test.udf

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * Created by zhaoshufen
  * User:  zhaoshufen
  * Date: 2017/11/17
  * Time: 13:30
  * To change this setting on:Preferences->editor->File and Code Templates->Include->File Header
  */
object UDFTest {
  def main(args: Array[String]): Unit = {

  }
  def udafTest01(): Unit ={
    val conf = new SparkConf()
      .setAppName("local")
      .setMaster("local[1]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    //sqlContext.udf.register()
    /**
      * 按照模板实现UDAF
      */
    class  MyUDAF extends UserDefinedAggregateFunction {
      /**
        * 该方法指定具体输入数据的类型
        * @return
        */
      override def inputSchema: StructType = StructType(Array(StructField("input", StringType, true)))

      /**
        * 在进行聚合操作的时候所要处理的数据的结果的类型
        * @return
        */
      override def bufferSchema: StructType = StructType(Array(StructField("count", IntegerType, true)))

      /**
        * 指定UDAF函数计算后返回的结果类型
        * @return
        */
      override def dataType: DataType = IntegerType

      override def deterministic: Boolean = true

      /**
        * 在Aggregate之前每组数据的初始化结果
        * @param buffer
        */
      override def initialize(buffer: MutableAggregationBuffer): Unit = {buffer(0) =0}

      /**
        * 在进行聚合的时候，每当有新的值进来，对分组后的聚合如何进行计算
        * 本地的聚合操作，相当于Hadoop MapReduce模型中的Combiner
        * @param buffer
        * @param input
        */
      override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        buffer(0) = buffer.getAs[Int](0) + 1
      }

      /**
        * 最后在分布式节点进行Local Reduce完成后需要进行全局级别的Merge操作
        * @param buffer1
        * @param buffer2
        */
      override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        buffer1(0) = buffer1.getAs[Int](0) + buffer2.getAs[Int](0)
      }


      /**
        * 返回UDAF最后的计算结果
        * @param buffer
        * @return
        */
      override def evaluate(buffer: Row): Any = buffer.getAs[Int](0)

    }
  }
}
