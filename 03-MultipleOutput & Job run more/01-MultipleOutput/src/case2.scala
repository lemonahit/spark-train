package com.zhaotao.SparkCoreHomework

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * Spark文件输出到指定目录
  * 实现路程：
  *           1. 完成了在deptno-30文件夹下输出两文件
  *                 解决方法：
  *                     将deptno-10 deptno-20  +  deptno-30下的内容分别使用partitionBy和coalesce算子
  *                     利用union算子，将它们的RDD联合起来
  *
  * Created by 陶 on 2017/11/10.
  */
object case2 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("case2").setMaster("local[2]")
    val sc = new SparkContext(conf)

    deleteFile(sc, "E:/multi_output")

    multipleOutTwo(sc)


    sc.stop()
  }

  /**
    * 输出的结构为：
    *   /multi_output/deptno-10
    *               part-xxx
    *   /multi_output/deptno-20
    *               part-xxx
    *   /multi_output/deptno-30
    *               part-xxx
    *               part-xxx
    *
    * @param sc
    */
  def multipleOutTwo(sc:SparkContext): Unit ={
    val newLine = sc.textFile("E:/data/emp1.txt").map(line => {
      val lines = line.split("\t")
      val key = "deptno-" + lines(7)
      val value = line
      (key, value)
    })

    val oneFileLOut = newLine.filter(line => {
      ! line._1.equals("deptno-30")
    })

    val twoFileOut = newLine.filter(line => {
      line._1.equals("deptno-30")
    })

    oneFileLOut//.coalesce(1)                                     // 两种写法都可以
               .partitionBy(new HashPartitioner(2))
               .union(twoFileOut.coalesce(2))
               .saveAsHadoopFile("E:/multi_output",
                                 classOf[String],
                                 classOf[String],
                                 classOf[RDDMultipleTextOutputFormat])

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