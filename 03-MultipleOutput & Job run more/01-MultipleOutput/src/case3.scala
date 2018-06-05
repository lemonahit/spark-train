package com.zhaotao.SparkCoreHomework

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * 使用sample算子进行取样，获得造成数据倾斜的key
  *
  * Created by 陶 on 2017/11/10.
  */
object case3 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("case3").setMaster("local[2]")
    val sc = new SparkContext(conf)

    deleteFile(sc, "E:/multi_output")

    dataSkew(sc)

    sc.stop()
  }

  // 使用sample算子进行取样
  def dataSkew(sc: SparkContext): Unit ={
    val data = sc.textFile("E:/data/emp3.txt").map(line => {
      val lines = line.split("\t")
      val key = "deptno-" + lines(7)
      val value = line
      (key, value)
    })
    val sampleData = data.sample(false, 0.3)
    val skewData = sampleData.countByKey()
    // 结果为(deptno-10,45)  (deptno-30,138)  (deptno-20,45)
    // 得知倾斜的key为deptno-30
    skewData.foreach(println)
  }

  /**
    * 删除路径
    *
    * @param sc
    * @param path
    */
  def deleteFile(sc:SparkContext, path:String): Unit ={
    val deletePath = new Path(path)
    val hadoopConf = sc.hadoopConfiguration
    val hdfs = FileSystem.get(hadoopConf)
    if(hdfs.exists(deletePath)){
      hdfs.delete(deletePath,true)
    }

  }

}

/**
  * 多目录输出
  * 参考blog：http://blog.csdn.net/dkcgx/article/details/52637899
  */
class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {

  // 根据key指定输出的文件夹
  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String = {
    key + "/" + name
  }

  // 输出时不输出key
  override def generateActualKey(key: Any, value: Any): Any = {
    NullWritable.get()
  }

}