package com.zhaotao.SparkCoreHomework

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Spark Broadcast 案例
  * 使用Spark实现mapjoin & commonjoin
  * 参考Blog地址：http://dongxicheng.org/framework-on-yarn/apache-spark-join-two-tables/
  *
  * Created by 陶 on 2017/11/7.
  */
object BroadcastApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("BroadcastApp").setMaster("local[2]")
    val sc = new SparkContext(conf)

    mapJoin(sc)
    //commonJoin(sc)
    sc.stop()
  }

  /**
    * 借助Broadcast实现MapJoin
    *
    * @param sc
    */
  def mapJoin(sc: SparkContext): Unit = {
    val peopleInfo = sc.parallelize(Array(("110", "huhuniao"), ("222", "loser"))).collectAsMap()
    val broadcastValue = sc.broadcast(peopleInfo)
    val peopleSchoolInfo = sc.parallelize(Array(("110", "ustc", "211"), ("111", "xxxx", "001")))
    peopleSchoolInfo.mapPartitions(iter => {
      val peopleMap = broadcastValue.value
      val arrayBuffer = ArrayBuffer[(String, String, String)]()
      iter.foreach { case (x, y, z) => {
          if (peopleMap.contains(x)) {
            arrayBuffer.+=((x, peopleMap.getOrElse(x, ""), y))
          }
        }
      }
      arrayBuffer.iterator
    }).foreach(println)
  }

  /**
    * 实现普通的CommonJoin
    *
    * @param sc
    */
  def commonJoin(sc: SparkContext): Unit = {
    val peopleInfo = sc.parallelize(Array(("110", "huhuniao"), ("222", "loser"))).map(x => (x._1, x))
    val peopleSchoolInfo = sc.parallelize(Array(("110", "ustc", "211"), ("111", "xxxx", "001"))).map(x => (x._1, x))

    // Join之后的数据为：
    // peopleInfo.join(peopleSchoolInfo)  ==>  (110,((110,huhuniao),(110,ustc,211)))
    peopleInfo.join(peopleSchoolInfo).map(x => {
      x._1 + "," + x._2._1._2 + "," + x._2._2._2
    }).foreach(println)
  }

}


