package com.zhaotao.SparkCoreHomework

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Spark Core实现作业的重跑 + 内容追加
  *
  * Created by 陶 on 2017/11/14.
  */
object case4 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("case4").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    val hadoopConf = new Configuration()
    // HDFS下使用该方式
//    val fileSystem = FileSystem.get(new URI("hdfs://192.168.26.131:8020"), hadoopConf)
    // 普通文件系统使用该方式
    val fileSystem = FileSystem.get(hadoopConf)

    val empPath = "E:/data/emp1.txt"
    val tmpPath = "E:/tmp/"
    val outPath = "E:/output/data/"

    multipleOutMore(sc, fileSystem, empPath, tmpPath, outPath)

    sc.stop()
  }

  def multipleOutMore(sc: SparkContext, fileSystem: FileSystem, empPath: String, tmpPath: String, outPath: String): Unit ={
    val pairRDD = sc.textFile(empPath).map(line => {
      val splitLines = line.split("\t")
      val key = "deptno=" + splitLines(7) + "/" + "hiredate=" + splitLines(4)
      val value = line
      (key, value)
    })
    deleteDir(tmpPath, fileSystem)
    // 先将数据存入/tmp目录
    pairRDD.saveAsHadoopFile(tmpPath,
                             classOf[String],
                             classOf[String],
                             classOf[RDDMultipleTextOutputFormat])

    val mapRDD = pairRDD.collectAsMap()
    val partitions = new mutable.HashSet[String]()
    // 将每条数据的key值添加到HashSet集合中
    // key值即deptno和hiredate两个分区的信息
    mapRDD.keys.foreach((partition: String) => partitions.add(partition))
    replaceTmpFile(fileSystem, tmpPath, outPath, partitions)
  }

  // 递归删除文件夹
  def deleteDir(tmpPath: String, fileSystem: FileSystem): Unit ={
    if (fileSystem.exists(new Path(tmpPath))) {
      fileSystem.delete(new Path(tmpPath), true)
    }
  }

  // 将tmpPath中的所有文件移植到/output/data目录下
  // 对不同分区的不同文件可以实现追加
  // 相同分区的相同文件则是进行替换
  def replaceTmpFile(fileSystem: FileSystem, tmpPath: String, outPath: String, partitions: mutable.HashSet[String]): Unit ={
    partitions.foreach(partition => {
      // 通过outPath和传入方法中的每个文件信息的变长集合partition去构建一个目录，之后通过/part-*去获取每个目录下的文件信息
      //      例如：/output/data/deptno=10/hiredate=xxx/part-*
      // 将/output/data下和跑出来数据分区信息重复的删除
      SparkHadoopUtil.get.globPath(new Path(outPath + partition + "/part-*")).map(fileSystem.delete(_, false))
      // 同上，通过tmpPath构建出每个文件的outPaths
      //      例如：/tmp/deptno=10/hiredate=xxx/part-*
      val outPaths = SparkHadoopUtil.get.globPath(new Path(tmpPath + partition + "/part-*"))
      outPaths.map(oldPath => {
        // 通过函数传入的outPath和分区信息构建出最终输出的finalPath
        //    例如：/output/data/deptno=10/hiredate=xxx
        val finalPath = new Path(outPath + partition)
        if (!fileSystem.exists(finalPath)) {
          fileSystem.mkdirs(finalPath)
        }
        // 完成数据的替换与追加操作
        fileSystem.rename(oldPath, finalPath)
      })
    })
  }

}

/**
  * 多目录输出
  */
class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {

  // 指定输出的文件夹
  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String = {
    key + "/" + name
  }

  // 输出时，文件中不输出key
  override def generateActualKey(key: Any, value: Any): Any = {
    NullWritable.get()
  }

}