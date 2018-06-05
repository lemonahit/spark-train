package com.zhaotao.SparkCoreHomework

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 使用Spark Accumulators完成Job的数据量处理
  * 统计emp表中NULL出现的次数以及正常数据的条数 & 打印正常数据的信息
  *
  * Created by 陶 on 2017/11/9.
  */
object AccumulatorsApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("AccumulatorsApp")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("E:/emp.txt")
    // long类型的累加器值
    val nullNum = sc.longAccumulator("NullNumber")
    val normalData = lines.filter(line => {
      var flag = true
      val splitLines = line.split("\t")
      for (splitLine <- splitLines){
        if ("".equals(splitLine)){
          flag = false
          nullNum.add(1)
        }
      }
      flag
    })

    // 使用cache方法，将RDD的第一次计算结果进行缓存；防止后面RDD进行重复计算，导致累加器的值不准确
    normalData.cache()
    // 打印每一条正常数据
    normalData.foreach(println)
    // 打印正常数据的条数
    println("NORMAL DATA NUMBER: " + normalData.count())
    // 打印emp表中NULL出现的次数
    println("NULL: " + nullNum.value)

    sc.stop()
  }

}
